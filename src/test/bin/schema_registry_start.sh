#!/bin/sh

echo "Starting pravega/schemaregistry docker container"
docker run -d --env STORE_TYPE=InMemory -p 9092:9092 pravega/schemaregistry:0.2.0-65.ba358e2-SNAPSHOT || exit 1
