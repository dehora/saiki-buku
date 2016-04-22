#!/usr/bin/env python3

from kazoo.client import KazooClient, NoNodeError
import requests
import os
import re


class BrokerIDByIp(object):

    def get_broker_unique_id(self, broker_id):
        zookeeper_connect_string = os.getenv('ZOOKEEPER_CONN_STRING')
        zk = KazooClient(hosts=zookeeper_connect_string, read_only=True)
        zk.start()
        try:
            ids = zk.get_children('/brokers/ids')
        except NoNodeError:
            return broker_id
        while broker_id in ids:
            broker_id = str(int(broker_id) + 1)
        zk.stop()
        return broker_id

    def get_by_ip(self):
        config_file = os.getenv('KAFKA_DIR') + '/config/server.properties'
        url = 'http://169.254.169.254/latest/dynamic/instance-identity/document'
        try:
            response = requests.get(url, timeout=5)
            json = response.json()
            myid = json['privateIp'].rsplit(".", 1)[1]
        except:
            myid = "1"

        broker_unique_id = self.get_broker_unique_id(myid)
        with open(config_file, mode='a', encoding='utf-8') as a_file:
            a_file.write('broker.id=' + broker_unique_id + '\n')

        return broker_unique_id

    def get_id(self, kafka_data_dir):
        return self.get_by_ip()


class BrokerIDAutoAssign(object):

    def get_id(self, kafka_data_dir):
        return self.get_from_meta(kafka_data_dir)

    def get_from_meta(self, kafka_data_dir):
        file_path = "{}/meta.properties".format(kafka_data_dir)
        if not os.path.isfile(file_path):
            return None
        broker_id_pattern = 'broker\.id=(\d+)'
        with open(file_path) as f:
            lines = f.readlines()
            for line in lines:
                match = re.search(broker_id_pattern, line)
                if match:
                    return match.group(1)
        return None


def get_broker_policy(policy):
    if policy == 'ip':
        return BrokerIDByIp()
    elif policy == 'auto':
        return BrokerIDAutoAssign()
    return BrokerIDByIp()