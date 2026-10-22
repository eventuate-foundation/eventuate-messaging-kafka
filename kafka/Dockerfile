# Kafka and Java come from Confluent's cp-kafka image (Apache 2 License), so the build downloads nothing.
# It keeps the interface of the previous image: the same environment variables, run-kafka.sh,
# bin/*.sh relative to the working directory, root user and healthcheck.
FROM confluentinc/cp-kafka:7.9.10
USER root
WORKDIR /usr/local/kafka
# Previous image ran Apache Kafka's bin/*.sh scripts; cp-kafka has them on the PATH without .sh
RUN mkdir bin config && \
    for f in /usr/bin/kafka-* /usr/bin/zookeeper-*; do \
      printf '#!/bin/bash\nexec %s "$@"\n' "$f" > "bin/$(basename "$f").sh" ; \
    done && \
    chmod +x bin/*.sh
EXPOSE 9092
COPY ./config/server.properties /usr/local/kafka-config/server.properties
COPY run-kafka.sh .
ENTRYPOINT []
CMD ./run-kafka.sh
HEALTHCHECK --interval=1s --retries=30 CMD (nc -z $(echo $KAFKA_LISTENERS | sed  -e 's?.*//??' -e 's/:/ /' )) || exit 1
