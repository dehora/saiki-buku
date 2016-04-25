import logging
import os
import subprocess
from time import sleep

import find_out_own_id
import generate_zk_conn_str
import wait_for_kafka_startup

kafka_dir = os.getenv('KAFKA_DIR')
kafka_data_dir = os.getenv('KAFKA_DATA_DIR')


def create_broker_properties(zk_conn_str):
    """Write the zookeeper connection string into the server.properties."""
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


def check_broker_id_in_zk(broker_id_policy, process, region):
    """
    Check endlessly for the Zookeeper Connection.

    This function checks endlessly if the broker is still registered in ZK
    (we observered running brokers but missing broker id's so we implemented this check)
    and if the ZK IP's changed (e.g. due to a node restart). If this happens a Kafka restart is enforced.
    """
    from kazoo.client import KazooClient
    zk_conn_str = os.getenv('ZOOKEEPER_CONN_STRING')
    broker_id_manager = find_out_own_id.get_broker_policy(broker_id_policy)
    broker_id = broker_id_manager.get_id(kafka_data_dir)
    logging.info("check broker id... {}".format(broker_id))

    if not broker_id:
        broker_id = wait_for_broker_id(broker_id_manager, kafka_data_dir)

    while True:
        check_kafka(region)

        new_zk_conn_str = generate_zk_conn_str.run(os.getenv('ZOOKEEPER_STACK_NAME'), region)
        if zk_conn_str != new_zk_conn_str:
            logging.warning("ZooKeeper connection string changed!")
            logging.warning("new ZK: " + new_zk_conn_str)
            logging.warning("old ZK: " + zk_conn_str)
            zk_conn_str = new_zk_conn_str
            os.environ['ZOOKEEPER_CONN_STRING'] = zk_conn_str
            create_broker_properties(zk_conn_str)
            from random import randint
            wait_to_stop = randint(1, 10)
            logging.info("Waiting " + str(wait_to_stop) + " seconds to stop kafka broker ...")
            sleep(wait_to_stop)
            process.terminate()
            process.wait()
            wait_to_restart = randint(10, 20)
            logging.info("Waiting " + str(wait_to_restart) + " seconds to restart kafka broker ...")
            sleep(wait_to_restart)
            logging.info("Restarting kafka broker with new ZooKeeper connection string ...")
            process = subprocess.Popen([kafka_dir + "/bin/kafka-server-start.sh",
                                        kafka_dir + "/config/server.properties"])
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
            logging.warning("I'm not in ZK registered, stopping kafka broker process!")
            zk.stop()
            process.terminate()
            process.wait()
            logging.info("Restarting kafka broker ...")
            process = subprocess.Popen([kafka_dir + "/bin/kafka-server-start.sh",
                                        kafka_dir + "/config/server.properties"])
            os.environ['WAIT_FOR_KAFKA'] = 'yes'


def wait_for_broker_id(broker_id_manager, data_dir):
    broker_id = None
    logging.info("waiting for broker id")
    while not broker_id:
        broker_id = broker_id_manager.get_id(data_dir)
        logging.info('read from {}, broker id {}'.format(data_dir, broker_id))
        sleep(5)

    logging.info("broker id is {}".format(broker_id))
    return broker_id


def check_kafka(region):
    if not region:
        logging.info("wait for local kafka in broker check")
        wait_for_kafka_startup.run('127.0.0.1')
    else:
        import requests
        try:
            if os.getenv('WAIT_FOR_KAFKA') != 'no':
                logging.info("wait for kafka in broker check")
                ip = requests.get('http://169.254.169.254/latest/dynamic/instance-identity/document').json()[
                    'privateIp']
                logging.info("wait for kafka in broker check - ip {}".format(ip))
                wait_for_kafka_startup.run(ip)
                logging.info("wait for kafka in broker check - ok")
                os.environ['WAIT_FOR_KAFKA'] = 'no'
        except:
            logging.info("exception on checking kafka")
