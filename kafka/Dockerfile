FROM amazoncorretto:8u322-al2
ENV KAFKA_VERSION=3.2.3
RUN yum install -y wget tar gzip nc gettext && \
  (wget -q -O - https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz |  tar -xzf - -C /usr/local) && \
    yum clean all && \
    rm -rf /var/cache/yum
WORKDIR /usr/local/kafka_2.13-${KAFKA_VERSION}
EXPOSE 9092
COPY ./config/server.properties /usr/local/kafka-config/server.properties
COPY run-kafka.sh .
#ENTRYPOINT ["/bin/bash", "-c", "./run-kafka.sh"]
CMD ./run-kafka.sh
HEALTHCHECK --start-period=20s --interval=15s CMD (nc -z $(echo $KAFKA_LISTENERS | sed  -e 's?.*//??' -e 's/:/ /' )) || exit 1
