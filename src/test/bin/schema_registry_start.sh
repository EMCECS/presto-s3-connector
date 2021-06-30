#!/bin/bash

echo "TEST456A"
echo "Pulling and starting pravega/schemaregistry docker container"
docker pull pravega/schemaregistry
docker run -d --name schemaregistry --env STORE_TYPE=InMemory -p 9092:9092 pravega/schemaregistry
docker ps
echo "TEST456B"

found=0
for i in 1 2 3 4 5 6 7 8 9 10; do
    curl -s localhost:9092 >/dev/null
    if [ $? -eq 0 ]; then
        found=1
        break;
    fi
    sleep 3
done

echo "TEST456C"

if [ $found -eq 0 ]; then
    echo "Image run failed: docker run -d --name schemaregistry --env STORE_TYPE=InMemory -p 9092:9092 pravega/schemaregistry"
    exit 1
fi

echo "TEST456D"

netstat -tunlp

