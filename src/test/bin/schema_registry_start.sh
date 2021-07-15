#!/bin/bash

# 
#  Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
# 
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# 
#  Note: This file contains changes from PrestoDb. Specifically the PrestoS3InputStream is inspired from the
#  Hive Connector.
#  https://github.com/prestodb/presto/blob/master/presto-hive/src/main/java/com/facebook/presto/hive/s3/PrestoS3FileSystem.java
# 

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
