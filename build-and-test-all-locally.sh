#! /bin/bash -e

./kafka/build-docker-kafka-multi-arch.sh

docker pull localhost:5002/eventuate-kafka:multi-arch-local-build

./build-and-test-all.sh
