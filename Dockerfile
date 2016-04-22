FROM registry.opensource.zalan.do/stups/python:3.5.0-12
MAINTAINER fabian.wollert@zalando.de teng.qiu@zalando.de

ENV KAFKA_VERSION="0.9.0.1" SCALA_VERSION="2.11" JOLOKIA_VERSION="1.3.3"
ENV KAFKA_TMP_DIR="/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
ENV KAFKA_DIR="/opt/kafka"

ENV CONFIG_PATHS="https://raw.githubusercontent.com/zalando/saiki-buku/master"
ENV SERVER_PROPERTIES="${CONFIG_PATHS}/server.properties"
ENV LOG4J_PROPERTIES="${CONFIG_PATHS}/log4j.properties"

ENV HEALTH_SERVER_PORT=${HEALTH_SERVER_PORT:-8080}

RUN apt-get update && apt-get install wget openjdk-8-jre -y --force-yes && apt-get clean
RUN pip3 install --upgrade kazoo boto3

ADD download_kafka.sh /tmp/download_kafka.sh
RUN chmod 777 /tmp/download_kafka.sh

RUN /tmp/download_kafka.sh
RUN tar xf /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt
RUN rm -rf /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz
RUN mv $KAFKA_TMP_DIR $KAFKA_DIR

RUN mkdir -p /data/kafka-logs
RUN chmod -R 777 /data/kafka-logs

ENV USE_JOLOKIA="yes"
RUN wget -O /tmp/jolokia-jvm-$JOLOKIA_VERSION-agent.jar http://search.maven.org/remotecontent?filepath=org/jolokia/jolokia-jvm/$JOLOKIA_VERSION/jolokia-jvm-$JOLOKIA_VERSION-agent.jar

RUN mkdir -p $KAFKA_DIR/logs/

RUN chmod -R 777 $KAFKA_DIR
WORKDIR $KAFKA_DIR

COPY src/. /tmp/
RUN chmod 777 /tmp/start_kafka_and_reassign_partitions.py

ADD scm-source.json /scm-source.json

ADD tail_logs_and_start.sh /tmp/tail_logs_and_start.sh
RUN chmod 777 /tmp/tail_logs_and_start.sh

ENTRYPOINT ["/bin/bash", "/tmp/tail_logs_and_start.sh"]

EXPOSE 9092 8004 ${HEALTH_SERVER_PORT}

