#!/bin/bash

echo "Pulling and starting pravega/schemaregistry docker container"
docker pull pravega/schemaregistry
docker run -d --name schemaregistry --env STORE_TYPE=InMemory -p 9092:9092 pravega/schemaregistry
sleep 5
docker ps
