#!/bin/bash

echo "Pulling and starting pravega/schemaregistry docker container"
docker pull pravega/schemaregistry
docker run -d --name schemaregistry --env STORE_TYPE=InMemory -p 9092:9092 pravega/schemaregistry
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

if [ $found -eq 0 ]; then
    echo "Image run failed: docker run -d --name schemaregistry --env STORE_TYPE=InMemory -p 9092:9092 pravega/schemaregistry"
    exit 1
fi
