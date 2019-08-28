if [ -z "$DOCKER_HOST_IP" ] ; then
    if [ -z "$DOCKER_HOST" ] ; then
      export DOCKER_HOST_IP=`hostname`
    else
      echo using ${DOCKER_HOST?}
      XX=${DOCKER_HOST%\:*}
      export DOCKER_HOST_IP=${XX#tcp\:\/\/}
    fi
fi

echo DOCKER_HOST_IP is $DOCKER_HOST_IP

export EVENTUATELOCAL_KAFKA_BOOTSTRAP_SERVERS=$DOCKER_HOST_IP:9092
export EVENTUATELOCAL_ZOOKEEPER_CONNECTION_STRING=$DOCKER_HOST_IP:2181
export EVENTUATE_EVENT_TRACKER_ITERATIONS=120
export START_KAFKA_COMMAND="docker-compose -f $PWD/docker-compose.yml start kafka"
export STOP_KAFKA_COMMAND="docker-compose -f $PWD/docker-compose.yml stop kafka"
