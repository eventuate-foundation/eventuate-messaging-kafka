#! /bin/bash -e

#docker build -t test-eventuateio-local-kafka .

docker buildx build --platform linux/amd64,linux/arm64 -t 182030608133.dkr.ecr.us-west-1.amazonaws.com/test-repo/eventuate-kafka --push .
