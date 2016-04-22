#!/usr/bin/env python3
"""General Kafka Start Script."""

import logging
import os
import signal
import subprocess
import sys
import time
import multiprocessing

import requests
import generate_zk_conn_str
import find_out_own_id
import rebalance_partitions

from broker_manager import check_broker_id_in_zk
from broker_manager import create_broker_properties
from health import HealthServer

kafka_dir = os.getenv('KAFKA_DIR')

logging.basicConfig(level=getattr(logging, 'INFO', None))

startup_delay = int(os.getenv('STARTUP_DELAY', '0'))
if startup_delay > 0:
    time.sleep(startup_delay)


def get_aws_region():
    try:
        logging.info("Checking if we are on AWS or not ...")
        response = requests.get('http://169.254.169.254/latest/dynamic/instance-identity/document', timeout=5)
        json = response.json()
        return json['region']
    except requests.exceptions.ConnectionError:
        logging.info("Seems like this is a local environment, we will run now in local mode")
        return None

region = get_aws_region()

zk_conn_str = generate_zk_conn_str.run(os.getenv('ZOOKEEPER_STACK_NAME'), region)
os.environ['ZOOKEEPER_CONN_STRING'] = zk_conn_str

logging.info("Got ZooKeeper connection string: " + zk_conn_str)


def get_remote_config(file, url):
    """Get a config from a remote location (e.g. Github)."""
    logging.info("getting " + file + " file from " + url)
    file_ = open(file, 'w')
    file_.write(requests.get(url).text)
    file_.close()


get_remote_config(kafka_dir + "/config/server.properties", os.getenv('SERVER_PROPERTIES'))
get_remote_config(kafka_dir + "/config/log4j.properties", os.getenv('LOG4J_PROPERTIES'))

create_broker_properties(zk_conn_str)
broker_id = find_out_own_id.get_broker_id_by_ip()


HealthServer().start()

reassign_process = None

if os.getenv('REASSIGN_PARTITIONS') == 'yes':
    logging.info("starting reassignment script")
    reassign_process = multiprocessing.Process(target=rebalance_partitions.run)
    reassign_process.start()

logging.info("starting kafka server ...")

kafka_options = "-server"
kafka_options += " -Dlog4j.configuration=file:" + kafka_dir + "/config/log4j.properties "
kafka_options += " -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=32M"

if os.getenv("USE_JOLOKIA") == 'yes':
    kafka_options += " -javaagent:/tmp/jolokia-jvm-" + os.getenv('JOLOKIA_VERSION') + "-agent.jar=host=0.0.0.0"

os.environ['KAFKA_OPTS'] = kafka_options

os.environ['KAFKA_JMX_OPTS'] = "-Dcom.sun.management.jmxremote=true " \
                               + "-Dcom.sun.management.jmxremote.authenticate=false " \
                               + "-Dcom.sun.management.jmxremote.ssl=false"

kafka_process = subprocess.Popen([kafka_dir + "/bin/kafka-server-start.sh",
                                  kafka_dir + "/config/server.properties"])

__ignore_sigterm = False


def sigterm_handler(signo, stack_frame):
    """Well yeah, what is this function doing?."""
    global __ignore_sigterm
    if not __ignore_sigterm:
        __ignore_sigterm = True
        sys.exit()


signal.signal(signal.SIGTERM, sigterm_handler)

try:
    check_process = multiprocessing.Process(target=check_broker_id_in_zk, args=[broker_id, kafka_process, region])
    check_process.start()

    if os.getenv('REASSIGN_PARTITIONS') == 'yes' and reassign_process:
        reassign_process.join()

    kafka_process.wait()
finally:
    if __ignore_sigterm:
        kafka_process.terminate()
        kafka_process.wait()
