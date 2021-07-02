#!/bin/bash

if [ ! -f /tmp/github.action.sr ]; then
    echo "Pulling and starting pravega/schemaregistry docker container"
    docker pull pravega/schemaregistry
    docker run -d --name schemaregistry --env STORE_TYPE=InMemory -p 9092:9092 pravega/schemaregistry
else
    echo "Schema registry docker container started by github action"
    rm -f /tmp/github.action.sr
fi

found=0
for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
    curl -s 127.0.0.1:9092 >/dev/null
    if [ $? -eq 0 ]; then
        found=1
        break;
    fi
    sleep 6
done

if [ $found -eq 0 ]; then
    echo "Image run failed: docker run -d --name schemaregistry --env STORE_TYPE=InMemory -p 9092:9092 pravega/schemaregistry"
    exit 1
fi
