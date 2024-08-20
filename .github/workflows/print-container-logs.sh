π#! /bin/bash -e

CONTAINER_IDS=$(docker ps -a -q)

for id in $CONTAINER_IDS ; do
  echo "\n--------------------"
  echo "logs of:\n"
  (docker ps -a -f "id=$id" || echo container gone: $id)
  echo "\n"
  (docker logs $id || echo container gone: $id)
  echo "--------------------\n"
done

mkdir -p ~/container-logs

docker ps -a > ~/container-logs/containers.txt

for name in $(docker ps -a --format "{{.Names}}") ; do
  (docker logs $name || echo container gone: $name) > ~/container-logs/${name}.log
done
