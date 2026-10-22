#! /bin/bash -e

# Combines the per-platform images pushed by build-docker-image-native.sh into one multi-platform image
# Usage: merge-docker-image-multi-arch.sh <image>

IMAGE=${1?}

docker buildx imagetools create -t $IMAGE $IMAGE-amd64 $IMAGE-arm64
