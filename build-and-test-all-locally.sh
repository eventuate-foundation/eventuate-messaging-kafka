#! /bin/bash -e

./gradlew testClasses

docker build -t eventuate-kafka:local-build kafka

./build-and-test-all.sh
