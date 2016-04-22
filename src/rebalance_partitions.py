import json
import logging
import os
from time import sleep

from kazoo.client import KazooClient, KazooState, NodeExistsError, NoNodeError
from kazoo.protocol.states import EventType


class NotEnoughBrokersException(Exception):
    def __init__(self):
        logging.warning("NotEnoughBrokersException")
        import sys
        sys.stdout.flush()


def state_listener(state):
    if state == KazooState.LOST:
        # Register somewhere that the session was lost
        pass
    elif state == KazooState.SUSPENDED:
        # Handle being disconnected from Zookeeper
        pass
    else:
        # Handle being connected/reconnected to Zookeeper
        pass


def readout_brokerids(zk):
    try:
        return zk.get_children("/brokers/ids")
    except NoNodeError:
        logging.error("there are no brokers registrated in this zookeeper cluster, therefore exiting")
        exit(1)


def readout_topics(zk):
    try:
        return zk.get_children("/brokers/topics")
    except NoNodeError:
        logging.info("there are no topics registrated in this zookeeper cluster")


def readout_topic_details(zk, topic):
    try:
        return json.loads(zk.get("/brokers/topics/" + topic)[0].decode('utf-8'))
    except NoNodeError:
        logging.info("no information for topic %s existing", topic)


def readout_partitions(zk, topic):
    try:
        return zk.get_children("/brokers/topics/" + topic + "/partitions")
    except NoNodeError:
        logging.info("there are no partitions for topic %s in this zookeeper cluster", topic)


def check_for_broken_partitions(zk_dict):
    result = {}
    for topic in zk_dict['topics']:
        logging.debug("checking topic: %s", topic['name'])
        for partition, brokers in topic['partitions'].items():
            logging.debug("checking partition: %s", partition)
            for part_broker_id in brokers:
                logging.debug("checking if this broker is still existing: %s", part_broker_id)
                if str(part_broker_id) not in zk_dict['broker']:
                    if topic['name'] not in result:
                        result[topic['name']] = {}
                    result[topic['name']][partition] = part_broker_id
                    logging.debug("no, it isn't ...")
                    break
                logging.debug("yes, it is ...")
    return result


def get_own_ip(region):
    if region:
        import requests
        return requests.get('http://169.254.169.254/latest/dynamic/instance-identity/document').json()['privateIp']
    else:
        return '127.0.0.1'


def update_broker_weigths(weights, brokers):
    for i, broker in enumerate(reversed(brokers)):
        if broker in weights:
            weights[broker] += 2 ** (i + i)


def get_broker_weights(zk_dict, target_brokers="all", ignore_existing=False):
    if target_brokers == "all":
        target_brokers = zk_dict['broker']
    weights = {int(broker): 0 for broker in target_brokers}
    if not ignore_existing:
        for topic in zk_dict['topics']:
            for brokers in topic['partitions'].values():
                update_broker_weigths(weights, brokers)
    return weights


def generate_json(zk_dict, topics_to_reassign="all", target_brokers="all"):
    ignore_existing = False

    new_broker_ass = False
    if set(zk_dict['broker']) - set(target_brokers) != set() and target_brokers != "all":
        avail_brokers_init = target_brokers
        new_broker_ass = True
        logging.debug("new broker assignment: " + str(avail_brokers_init))
    else:
        avail_brokers_init = zk_dict['broker']

    if topics_to_reassign == "all" or new_broker_ass is True:
        # logging.info("reassigning all topics")
        topics_to_reassign = {}
        for topic in zk_dict['topics']:
            topics_to_reassign[topic['name']] = {}
            for partition in topic['partitions']:
                topics_to_reassign[topic['name']][partition] = 0
        ignore_existing = True
    logging.debug("topics_to_reassign:")
    logging.debug(topics_to_reassign)

    tmp_topic_dict = {t['name']: t['partitions'] for t in zk_dict['topics']}

    # this is needed for creating new topics (currently in pemetaan)
    for topic_to_reassign, value in topics_to_reassign.items():
        if topic_to_reassign not in tmp_topic_dict:
            tmp_topic_dict[topic_to_reassign] = {}
            for partition, brokers in value.items():
                tmp_topic_dict[topic_to_reassign][str(partition)] = brokers

    if len(topics_to_reassign) > 0:
        logging.info("topics_to_reassign found, generating new assignment pattern")
        logging.info("reading out broker id's")

        final_result = {'version': 1, 'partitions': []}
        logging.info("generating now ")
        weights = get_broker_weights(zk_dict, avail_brokers_init, ignore_existing)
        for topic, partitions in topics_to_reassign.items():
            replication_factor = sorted(map(len, tmp_topic_dict[topic].values()))[0]

            logging.debug("Available Brokers: %s", len(avail_brokers_init))
            logging.debug("Replication Factor: %s", replication_factor)

            if len(avail_brokers_init) < replication_factor:
                raise NotEnoughBrokersException

            for partition in partitions:
                logging.debug("finding new brokers for topic: %s, partition: %s", topic, partition)
                broker_list = [b for b, w in sorted(weights.items(), key=lambda v: v[1])][:replication_factor]
                final_result['partitions'].append({'topic': topic,
                                                   'partition': int(partition),
                                                   'replicas': broker_list})
                update_broker_weigths(weights, broker_list)
        return final_result
    else:
        return {}


def write_json_to_zk(zk, final_result):
    watcher_event = zk.handler.event_object()

    def reassign_partitions_watcher(event):
        if event.type == EventType.DELETED:
            watcher_event.set()
        else:
            if zk.exists('/admin/reassign_partitions', reassign_partitions_watcher) is None:
                watcher_event.set()

    logging.info("writing reassigned partitions in ZK")
    count_steps_left = len(final_result['partitions'])
    for step in final_result['partitions']:
        if count_steps_left % 20 == 0 or count_steps_left == len(final_result['partitions']):
            logging.info("steps left: %s", count_steps_left)
        logging.info("trying to write zk node for repairing %s", step)
        done = False

        while not done:
            try:
                zk.create("/admin/reassign_partitions",
                          json.dumps({'version': 1, 'partitions': [step]}, separators=(',', ':')).encode('utf-8'))
                done = True
                logging.info("done")
                count_steps_left -= 1
                watcher_event.clear()
                if zk.exists('/admin/reassign_partitions', reassign_partitions_watcher) is not None:
                    watcher_event.wait(300)
            except NodeExistsError:
                try:
                    watcher_event.clear()
                    check = zk.get("/admin/reassign_partitions", reassign_partitions_watcher)
                    if check[0] == b'{"version": 1, "partitions": []}':
                        zk.delete("/admin/reassign_partitions", recursive=True)
                        continue

                    for timeout_count in range(0, 6):
                        # only output message every 10mins
                        logging.info("there seems to be a reassigning already taking place: %s",
                                     check[0].decode('utf-8'))
                        logging.info("waiting ...")
                        watcher_event.wait(300)
                        if watcher_event.isSet():
                            break
                except NoNodeError:
                    pass
                    # logging.info("NoNodeError")
        if done is False:
            logging.warning("Reassignment was not successfull due to timeout issues of the previous reassignment")
            break


def get_zk_dict(zk):
    result = {'topics': [], 'broker': readout_brokerids(zk)}
    for topic in readout_topics(zk):
        result['topics'].append({'name': topic, 'partitions': readout_topic_details(zk, topic)['partitions']})
    return result


def connect_to_zk():
    zookeeper_connect_string = os.getenv('ZOOKEEPER_CONN_STRING')
    zk = KazooClient(hosts=zookeeper_connect_string)
    zk.start()
    zk.add_listener(state_listener)
    logging.info("connected to Zookeeper")
    return zk


def run(region):
    import wait_for_kafka_startup
    logging.info("waiting for kafka to start up")
    if os.getenv('WAIT_FOR_KAFKA') != 'no':
        wait_for_kafka_startup.run(get_own_ip(region))
    else:
        sleep(10)

    logging.info("kafka port is open, continuing")

    zk = connect_to_zk()
    zk_dict = get_zk_dict(zk)

    logging.info("checking for broken topics")
    result = generate_json(zk_dict, topics_to_reassign=check_for_broken_partitions(zk_dict))
    if result != {}:
        logging.info("there are %s partitions to repair", len(result['partitions']))
        logging.debug(result)
        if os.getenv('WRITE_TO_JSON') != 'no':
            logging.info("writing to ZooKeeper ...")
            write_json_to_zk(zk, result)
    else:
        logging.info("no broken topics found, no JSON generated")

        if any(weight == 0 for weight in get_broker_weights(zk_dict).values()):
            logging.info("there are unused brokers, reassigning all topics ...")
            result = generate_json(zk_dict)
            if result != {}:
                logging.info("JSON generated")
                logging.debug(result)
                if os.getenv('WRITE_TO_JSON') != 'no':
                    write_json_to_zk(zk, result)
        else:
            logging.info("no unused Broker found, no JSON generated")

    zk.stop()
    logging.info("exiting")
