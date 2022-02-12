FROM amazoncorretto:17.0.1-al2
ENV KAFKA_VERSION=kafka_2.13-3.0.0
RUN yum install -y wget tar gzip && \
  echo https://dlcdn.apache.org/kafka/3.0.0/${KAFKA_VERSION}.tgz && \
  wget -q -O - https://dlcdn.apache.org/kafka/3.0.0/${KAFKA_VERSION}.tgz |  tar -xzf - -C /usr/local
WORKDIR /usr/local/${KAFKA_VERSION}
EXPOSE 9092
COPY ./config/server.properties /usr/local/kafka-config/server.properties
COPY run-kafka.sh .
ENTRYPOINT ["/bin/bash", "-c", "./run-kafka.sh"]
