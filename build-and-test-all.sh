#! /bin/bash

set -e

export EVENTUATE_EVENT_TRACKER_ITERATIONS=120

./gradlew testClasses

docker="./gradlew compose"

./kafka/build-docker-kafka-multi-arch.sh

docker pull ${KAFKA_MULTI_ARCH_IMAGE:-${DOCKER_HOST_NAME:-host.docker.internal}:5002/eventuate-kafka:multi-arch-local-build}

${docker}Down
${docker}Up

./gradlew $GRADLE_OPTIONS cleanTest build $GRADLE_TASK_OPTIONS

${docker}Down
