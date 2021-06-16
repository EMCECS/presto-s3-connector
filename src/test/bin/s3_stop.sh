#!/bin/sh

s3image=scality/s3server

for image in `docker ps | awk '{print $1}' | grep -v CONTAINER`
do
    if [ ! -z "`docker inspect $image | grep '"Image":' | grep $s3image`" ]
    then
        echo "Stop container $image" && docker stop $image
    fi
done

for image in `docker ps -a | awk '{print $1}' | grep -v CONTAINER`
do
    if [ ! -z "`docker inspect $image | grep '"Image":' | grep $s3image`" ]
    then
        echo "Remove container $image" && docker rm $image
    fi
done
