#!/bin/sh

minioimage=minio/minio 
miniomcimage=minio/mc

for image in `docker ps | awk '{print $1}' | grep -v CONTAINER`
do
    if [ ! -z "`docker inspect $image | grep '"Image":' | grep $minioimage`" ]
    then
        echo "Stop container $image" && docker stop $image
    elif [ ! -z "`docker inspect $image | grep '"Image":' | grep $miniomcimage`" ]
    then
        echo "Stop container $image" && docker stop $image
    fi
done

for image in `docker ps -a | awk '{print $1}' | grep -v CONTAINER`
do
    if [ ! -z "`docker inspect $image | grep '"Image":' | grep $minioimage`" ]
    then
        echo "Remove container $image" && docker rm $image
    elif [ ! -z "`docker inspect $image | grep '"Image":' | grep $miniomcimage`" ]
    then
        echo "Remove container $image" && docker rm $image
    fi
done
