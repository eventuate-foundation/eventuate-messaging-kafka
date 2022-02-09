#! /bin/bash

./kafka/build-docker-kafka-multi-arch.sh

docker pull ${KAFKA_MULTI_ARCH_IMAGE:-${DOCKER_HOST_NAME:-host.docker.internal}:5002/eventuate-kafka:multi-arch-local-build}

./build-and-test-all.sh
