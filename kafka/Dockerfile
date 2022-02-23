FROM amazoncorretto:8u322-al2
ENV KAFKA_VERSION=kafka_2.13-2.8.1
RUN yum install -y wget tar gzip nc gettext && \
  echo https://dlcdn.apache.org/kafka/3.0.0/${KAFKA_VERSION}.tgz && \
  (wget -q -O - https://dlcdn.apache.org/kafka/2.8.1/${KAFKA_VERSION}.tgz |  tar -xzf - -C /usr/local) && \
    yum clean all && \
    rm -rf /var/cache/yum
WORKDIR /usr/local/${KAFKA_VERSION}
EXPOSE 9092
COPY ./config/server.properties /usr/local/kafka-config/server.properties
COPY run-kafka.sh .
ENTRYPOINT ["/bin/bash", "-c", "./run-kafka.sh"]
HEALTHCHECK --start-period=20s --interval=15s CMD (nc -z $(echo $KAFKA_LISTENERS | sed  -e 's?.*//??' -e 's/:/ /' )) || exit 1
