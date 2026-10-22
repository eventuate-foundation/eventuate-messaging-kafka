#! /bin/bash -e

set -e

EFFECTIVE_KAFKA_CONFIG_DIR=./config

export KAFKA_LOG_DIRS=${KAFKA_LOG_DIRS:-/tmp/kafka-logs}

# Substitutes the variables that server.properties uses. cp-kafka has no envsubst.
sed -e "s|\${KAFKA_LISTENERS}|${KAFKA_LISTENERS}|g" \
    -e "s|\${KAFKA_ADVERTISED_LISTENERS}|${KAFKA_ADVERTISED_LISTENERS}|g" \
    -e "s|\${KAFKA_LISTENER_SECURITY_PROTOCOL_MAP}|${KAFKA_LISTENER_SECURITY_PROTOCOL_MAP}|g" \
    -e "s|\${KAFKA_INTER_BROKER_LISTENER_NAME}|${KAFKA_INTER_BROKER_LISTENER_NAME}|g" \
    -e "s|\${KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR}|${KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR}|g" \
    -e "s|\${KAFKA_LOG_DIRS}|${KAFKA_LOG_DIRS}|g" \
    -e "s|\${KAFKA_ZOOKEEPER_CONNECT}|${KAFKA_ZOOKEEPER_CONNECT}|g" \
    < /usr/local/kafka-config/server.properties > $EFFECTIVE_KAFKA_CONFIG_DIR/server.properties

exec bin/kafka-server-start.sh $EFFECTIVE_KAFKA_CONFIG_DIR/server.properties
