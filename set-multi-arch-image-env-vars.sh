export MULTI_ARCH_TAG=test-build-${CIRCLE_SHA1?}
export BUILDX_PUSH_OPTIONS=--push

export KAFKA_MULTI_ARCH_IMAGE=eventuateio/eventuate-kafka:$MULTI_ARCH_TAG
