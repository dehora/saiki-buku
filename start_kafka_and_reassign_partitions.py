#!/usr/bin/env python3

import subprocess
import os
import rebalance_partitions
import logging
import requests
import signal
import sys
import find_out_own_id
from multiprocessing import Pool
import wait_for_kafka_startup
from kazoo.client import KazooClient
from health import HealthServer
import boto3


def generate_zk_conn_str(stack_name, region=None):
    return generate_zk_conn_str_from_stack_name(stack_name, region) + os.getenv('ZOOKEEPER_PREFIX', '')


def generate_zk_conn_str_from_stack_name(stack_name, region=None):
    private_ips = []

    if region is not None:
        elb = boto3.client('elb', region_name=region)
        ec2 = boto3.client('ec2', region_name=region)

        response = elb.describe_instance_health(LoadBalancerName=stack_name)

        for instance in response['InstanceStates']:
            if instance['State'] == 'InService':
                private_ips.append(ec2.describe_instances(
                    InstanceIds=[instance['InstanceId']])['Reservations'][0]['Instances'][0]['PrivateIpAddress'])

    else:
        private_ips = [stack_name]

    zk_conn_str = ''
    for ip in private_ips:
        zk_conn_str += ip + ':2181,'

    return zk_conn_str[:-1]

kafka_dir = os.getenv('KAFKA_DIR')

logging.basicConfig(level=getattr(logging, 'INFO', None))

try:
    logging.info("Checking if we are on AWS or not ...")
    response = requests.get('http://169.254.169.254/latest/dynamic/instance-identity/document', timeout=5)
    json = response.json()
    region = json['region']
except requests.exceptions.ConnectionError:
    logging.info("Seems like this is a local environment, we will run now in local mode")
    region = None

zk_conn_str = generate_zk_conn_str(os.getenv('ZOOKEEPER_STACK_NAME'), region)
os.environ['ZOOKEEPER_CONN_STRING'] = zk_conn_str

logging.info("Got ZooKeeper connection string: " + zk_conn_str)


def get_remote_config(file, url):
    logging.info("getting " + file + " file from " + url)
    file_ = open(file, 'w')
    file_.write(requests.get(url).text)
    file_.close


def create_broker_properties(zk_conn_str):
    with open(kafka_dir + '/config/server.properties', "r+") as f:
        lines = f.read().splitlines()
        f.seek(0)
        f.truncate()
        f.write('zookeeper.connect=' + zk_conn_str + '\n')
        for line in lines:
            if not line.startswith("zookeeper.connect"):
                f.write(line + '\n')
        f.close()

    logging.info("Broker properties generated with zk connection str: " + zk_conn_str)

get_remote_config(kafka_dir + "/config/server.properties", os.getenv('SERVER_PROPERTIES'))
get_remote_config(kafka_dir + "/config/log4j.properties", os.getenv('LOG4J_PROPERTIES'))

create_broker_properties(zk_conn_str)

# create chroot if not existent

zk = KazooClient(hosts=generate_zk_conn_str_from_stack_name(os.getenv('ZOOKEEPER_STACK_NAME'), region))
zk.start()
if (zk.exists(os.getenv('ZOOKEEPER_PREFIX'), watch=None) is None):
    zk.create(os.getenv('ZOOKEEPER_PREFIX'))
zk.stop()
zk.close()

broker_id = find_out_own_id.run()


def check_broker_id_in_zk(broker_id, process):
    import requests
    from time import sleep
    zk_conn_str = os.getenv('ZOOKEEPER_CONN_STRING')
    while True:
        if os.getenv('WAIT_FOR_KAFKA') != 'no':
            ip = requests.get('http://169.254.169.254/latest/dynamic/instance-identity/document').json()['privateIp']
            wait_for_kafka_startup.run(ip)
            os.environ['WAIT_FOR_KAFKA'] = 'no'

        new_zk_conn_str = generate_zk_conn_str(os.getenv('ZOOKEEPER_STACK_NAME'), region)
        if zk_conn_str != new_zk_conn_str:
            logging.warning("ZooKeeper connection string changed!")
            zk_conn_str = new_zk_conn_str
            os.environ['ZOOKEEPER_CONN_STRING'] = zk_conn_str
            create_broker_properties(zk_conn_str)
            from random import randint
            wait_to_restart = randint(1, 20)
            logging.info("Waiting " + str(wait_to_restart) + " seconds to restart kafka broker ...")
            sleep(wait_to_restart)
            process.kill()
            logging.info("Restarting kafka broker with new ZooKeeper connection string ...")
            process = subprocess.Popen([kafka_dir
                                        + "/bin/kafka-server-start.sh", kafka_dir
                                        + "/config/server.properties"])
            os.environ['WAIT_FOR_KAFKA'] = 'yes'
            continue

        zk = KazooClient(hosts=zk_conn_str)
        zk.start()
        try:
            zk.get("/brokers/ids/" + broker_id)
            logging.info("I'm still in ZK registered, all good!")
            sleep(60)
            zk.stop()
        except:
            logging.warning("I'm not in ZK registered, killing kafka broker process!")
            zk.stop()
            process.kill()
            logging.info("Restarting kafka broker ...")
            process = subprocess.Popen([kafka_dir
                                        + "/bin/kafka-server-start.sh", kafka_dir
                                        + "/config/server.properties"])
            os.environ['WAIT_FOR_KAFKA'] = 'yes'

HealthServer().start()

pool = Pool()

if os.getenv('REASSIGN_PARTITIONS') == 'yes':
    logging.info("starting reassignment script")
    pool.apply_async(rebalance_partitions.run)

logging.info("starting kafka server ...")
os.environ['KAFKA_OPTS'] = "-server " \
                           + "-Dlog4j.configuration=file:" + kafka_dir + "/config/log4j.properties " \
                           + "-javaagent:/tmp/jolokia-jvm-" + os.getenv('JOLOKIA_VERSION') + "-agent.jar=host=0.0.0.0" \
                           + " -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=32M"
# + "-Xmx512M " \
os.environ['KAFKA_JMX_OPTS'] = "-Dcom.sun.management.jmxremote=true " \
                               + "-Dcom.sun.management.jmxremote.authenticate=false " \
                               + "-Dcom.sun.management.jmxremote.ssl=false"
kafka_process = subprocess.Popen([kafka_dir + "/bin/kafka-server-start.sh",
                                  kafka_dir + "/config/server.properties"])


__ignore_sigterm = False


def sigterm_handler(signo, stack_frame):
    global __ignore_sigterm
    if not __ignore_sigterm:
        __ignore_sigterm = True
        sys.exit()


signal.signal(signal.SIGTERM, sigterm_handler)


try:
    pool.apply_async(check_broker_id_in_zk, [broker_id, kafka_process])

    if os.getenv('REASSIGN_PARTITIONS') == 'yes':
        pool.close()
        pool.join()

    kafka_process.wait()
finally:
    if __ignore_sigterm:
        kafka_process.terminate()
        kafka_process.wait()
