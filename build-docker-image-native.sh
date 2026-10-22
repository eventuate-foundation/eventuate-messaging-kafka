#! /bin/bash -e

# Builds an image for the Docker host's platform and pushes it with a per-platform tag
# Usage: build-docker-image-native.sh <image> <dockerfile> <context>

IMAGE=${1?}
DOCKERFILE=${2?}
CONTEXT=${3?}

ARCH=$(docker version --format '{{.Server.Arch}}')

docker build -t $IMAGE-$ARCH -f $DOCKERFILE $CONTEXT
docker push $IMAGE-$ARCH
