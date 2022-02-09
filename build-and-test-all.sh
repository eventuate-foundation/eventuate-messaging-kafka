#! /bin/bash

set -e

export EVENTUATE_EVENT_TRACKER_ITERATIONS=120

./gradlew testClasses

docker="./gradlew compose"

${docker}Down
${docker}Up

./gradlew $GRADLE_OPTIONS cleanTest build $GRADLE_TASK_OPTIONS

${docker}Down
