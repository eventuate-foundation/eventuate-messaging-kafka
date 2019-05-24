#! /bin/bash

set -e

. ./set-env.sh

docker-compose down -v

docker-compose up --build -d

sleep 10

./gradlew $GRADLE_OPTIONS cleanTest build $GRADLE_TASK_OPTIONS

docker-compose down -v