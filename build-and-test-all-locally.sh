#! /bin/bash -e

./gradlew testClasses

./kafka/build-docker-kafka-multi-arch.sh

./build-and-test-all.sh
