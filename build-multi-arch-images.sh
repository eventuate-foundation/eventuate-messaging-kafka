#! /bin/bash -e

docker login -u ${DOCKER_USER_ID?} -p ${DOCKER_PASSWORD?}

./kafka/build-docker-kafka-multi-arch.sh
