#!/bin/bash

if [ ! -f /tmp/github.action.sr ]; then
    echo "Pulling and starting pravega/schemaregistry docker container"
    docker pull pravega/schemaregistry
    docker run -d --name schemaregistry --env STORE_TYPE=InMemory -p 9092:9092 pravega/schemaregistry
else
    rm -f /tmp/github.action.sr
fi

echo "TEST456A"

docker ps

found=0
set -B                  # enable brace expansion
for i in {1..30}; do
    curl -s localhost:9092 >/dev/null
    if [ $? -eq 0 ]; then
        found=1
        break;
    fi
    sleep 1
done

echo "TEST456B"

if [ $found -eq 0 ]; then
    echo "Image run failed: docker run -d --name schemaregistry --env STORE_TYPE=InMemory -p 9092:9092 pravega/schemaregistry"
    exit 1
fi

echo "TEST456C"

netstat -tunlp

