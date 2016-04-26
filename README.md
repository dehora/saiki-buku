Introduction
============
Buku (javanese for book) is a [Kafka](http://kafka.apache.org/) Appliance for [STUPS](https://stups.io/).

Buku uses port ```8004``` as ```JMX_PORT```.

Usage
=====
After building the docker image, start Buku like this:
```
docker run -d \
  -e ZOOKEEPER_STACK_NAME=localhost \
  -e JMX_PORT=8004 \
  -e EXHIBITOR_HOST=localhost \
  -e ZOOKEEPER_PREFIX=/buku \
  -p 8004:8004 \
  -p 9092:9092 \
  --net=host \
  <IMAGE_ID>
```
Docker run option ```--net=host``` is needed, so kafka can bind the interface from the host, to listen for leader elections. Ref. https://docs.docker.com/articles/networking/#how-docker-networks-a-container

For local test, ```ZOOKEEPER_STACK_NAME``` should be set to the DNS name or IP adresse of one of your ZooKeeper node, such as in this example, ZooKeeper is running on localhost, you can set it with the host alias ```ZOOKEEPER_STACK_NAME=localhost```, or the local loopback IP like ```ZOOKEEPER_STACK_NAME=127.0.0.1```

###### Docker compose

You can also use ```docker-compose``` in order to run the appliance locally. Just run:
```
docker-compose up
```

Deployment with STUPS toolbox
-----------------------------

###### Docker Image

We advise to use the official release in the OpenSource Registry of Zalando. You can find out the latest here:
```
curl -s https://registry.opensource.zalan.do/teams/saiki/artifacts/buku/tags | jq "sort_by(.created)"
```

If you want to build your own image see here: http://docs.stups.io/en/latest/user-guide/deployment.html#prepare-the-deployment-artifact

###### Register the Buku app in Yourturn/Kio

Docs: http://docs.stups.io/en/latest/components/yourturn.html

if needed, you need to adjust later in the yaml-file the Stackname or the application_id to suit the one you have put in Yourturn/Kio

###### get our YAML File with the senza definition
```
wget https://raw.githubusercontent.com/zalando/saiki-buku/master/buku.yaml
```

###### execute senza with the definition file

```
senza create buku.yaml <STACK_VERSION> <DOCKER_IMAGE_WITH_VERSION_TAG> <MINT_BUCKET> <SCALYR_LOGGING_KEY> <APPLICATION_ID> <ZOOKEEPER_STACK_NAME> <Hosted_Zone> [--region AWS_REGION]
```

A real world example would be:
```
senza create buku.yaml 1 pierone.example.org/myteam/buku:0.1-SNAPSHOT example-stups-mint-some_id-eu-west-1 some_scalyr_key buku zookeeper-stack example.org. --region eu-west-1
```

An autoscaling group will be created and Buku docker container will be running on all of the EC2 instances in this autoscaling group.

Your Kafka Producer/Consumer can connect to this Buku cluster with its Route53 DNS name: ```<STACK_NAME>.<Hosted_Zone>```, such as: ```buku.example.org```. This is a CNAME record with value of Buku's AppLoadBalancer (ELB), this LoadBalancer is an internal LoadBalancer, so that means, in order to access this Buku cluster, your Producer or Consumer also need to be deployed in the same region's VPC in AWS.

Check the STUPS documention for additional options:
http://docs.stups.io

## Environmental variables

- ```BROKER_ID_POLICY``` the algorithm for broker id, could be ```ip``` or ```auto```
- ```KAFKA_HEAP_OPTS``` additional Kafka heap options


## Monitoring / Additions

There are additional Services running to help us with some tasks:

### Jolokia HTTP Endpoint

to make the JMX metrics available on a REST endpoint, we included the [Jolokia](https://jolokia.org/) Service as JVM Agent in the startup. You can query the Kafka JMX Metrics on this endpoint:

read
```
curl https://instance.example.org:8778/jolokia/read/kafka.server:name=BytesInPerSec,type=BrokerTopicMetrics,topic=topic_name
```

search
```
curl https://instance.example.org:8778/jolokia/search/kafka.*:name=*,type=Broker*
```

### Additional Health Endpoint

We also need additional Metrics besides the ones JMX offers us. We implemented a custom endpoint to serve these right now.

#### Broken Partitions

This healthcheck simply checks that there are not partitions which are stored on brokers which are not registered in zookeeper. As a result it returns array of "dead broker" id's. If there are no dead brokers array would be empty.

Endpoint:
```
curl https://instance.example.org:8080/
```
