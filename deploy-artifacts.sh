#! /bin/bash -e

BRANCH=$(git rev-parse --abbrev-ref HEAD)

if ! [[  $BRANCH =~ ^[0-9]+ ]] ; then
  echo Not release $BRANCH - no PUSH
  exit 0
elif [[  $BRANCH =~ RELEASE$ ]] ; then
  BINTRAY_REPO_TYPE=release
elif [[  $BRANCH =~ M[0-9]+$ ]] ; then
    BINTRAY_REPO_TYPE=milestone
elif [[  $BRANCH =~ RC[0-9]+$ ]] ; then
    BINTRAY_REPO_TYPE=rc
else
  echo cannot figure out bintray for this branch $BRANCH
  exit -1
fi

echo BINTRAY_REPO_TYPE=${BINTRAY_REPO_TYPE}

VERSION=$BRANCH

$PREFIX ./gradlew -P version=${VERSION} \
  -P bintrayRepoType=${BINTRAY_REPO_TYPE} \
  -P deployUrl=https://dl.bintray.com/eventuateio-oss/eventuate-maven-${BINTRAY_REPO_TYPE} \
  testClasses bintrayUpload

DOCKER_REPO=eventuateio
DOCKER_COMPOSE_PREFIX=eventuatemessagingkafka_
DOCKER_REMOTE_PREFIX=eventuate-

function tagAndPush() {
  LOCAL=$1
  REMOTE=$2
  FULL_REMOTE=${DOCKER_REMOTE_PREFIX}${REMOTE}
  $PREFIX docker tag ${DOCKER_COMPOSE_PREFIX?}$LOCAL $DOCKER_REPO/${FULL_REMOTE}:$VERSION
  $PREFIX docker tag ${DOCKER_COMPOSE_PREFIX?}$LOCAL $DOCKER_REPO/${FULL_REMOTE}:latest
  echo Pushing $DOCKER_REPO/${FULL_REMOTE}:$VERSION
  $PREFIX docker push $DOCKER_REPO/${FULL_REMOTE}:$VERSION
  $PREFIX docker push $DOCKER_REPO/${FULL_REMOTE}:latest
}

$PREFIX docker login -u ${DOCKER_USER_ID?} -p ${DOCKER_PASSWORD?}

docker images

tagAndPush "kafka" "kafka"