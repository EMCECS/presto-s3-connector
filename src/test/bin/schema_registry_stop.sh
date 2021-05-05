#!/bin/sh

image=pravega/schemaregistry

RUNNING=$(docker ps | grep $image | awk '{print $1}' | tr '\012' ' ')
[ ! -z "$RUNNING" ] && echo "Stop container" && docker stop $RUNNING

STOPPED=$(docker ps -a | grep $image | awk '{print $1}' | tr '\012' ' ')
[ ! -z "$STOPPED" ] && echo "Remove container" && docker rm $STOPPED

# IMAGES=$(docker images | grep $image | awk '{print $3}' | tr '\012' ' ')
# [[ ! -z "$IMAGES" ]] && docker rmi $IMAGES 
