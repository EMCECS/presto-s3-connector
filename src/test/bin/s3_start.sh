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

export S3_DOCKER_NAME=test-s3
export S3_DOCKER_PORT=8000
export S3_ACCESS_KEY="accessKey1"
export S3_SECRET_KEY="verySecretKey1"
export S3_BUCKET=testbucket
SCRIPT=$(readlink -f $0)
SCRIPTDIR=$(dirname $SCRIPT)
export CSV=$(readlink --canonicalize $SCRIPTDIR/../resources/medical.csv)
export CSV1=$(readlink --canonicalize $SCRIPTDIR/../resources/names.csv)
export CSV2=$(readlink --canonicalize $SCRIPTDIR/../resources/grades.csv)
export JSON1=$(readlink --canonicalize $SCRIPTDIR/../resources/json_datafile)
export JSON2=$(readlink --canonicalize $SCRIPTDIR/../resources/json_datafile)
export JSON3=$(readlink --canonicalize $SCRIPTDIR/../resources/types.json)
export AVRODATA1=$(readlink --canonicalize $SCRIPTDIR/../resources/avro_datafile)
export TXTFILE=$(readlink --canonicalize $SCRIPTDIR/../resources/datafile.txt)
export PARQUET=$(readlink --canonicalize $SCRIPTDIR/../resources/customerfile)
export PARQUET1=$(readlink --canonicalize $SCRIPTDIR/../resources/storefile)
export CSVDIR=$(dirname $CSV)

echo "Starting s3 docker container"
docker pull scality/s3server
docker run -d --name s3server -p 8000:8000 scality/s3server || exit 1

# Wait for container to listen on port 8000
found=0
for i in `seq 20`; do
    curl -s 127.0.0.1:8000 >/dev/null
    if [ $? -eq 0 ]; then
        found=1
        break;
    fi
    sleep 15
done

if [ $found -eq 0 ]; then
    echo "Image run failed: docker run -d --name s3server -p $S3_DOCKER_PORT:$S3_DOCKER_PORT scality/s3server"
    exit 1
fi

if [ ! -f /tmp/s3curl/s3curl.pl ]; then
    DIR=`pwd`
    cd  /tmp
    git clone https://github.com/EMCECS/s3curl.git
    chmod +x /tmp/s3curl/s3curl.pl
    cd $DIR
fi

if [ -f ~/.s3curl ]; then
    mv ~/.s3curl ~/.s3curl.bak.$$
fi

cat > ~/.s3curl << EOF
%awsSecretAccessKeys = (
    scality => {
        id => '$S3_ACCESS_KEY',
        key => '$S3_SECRET_KEY',
    },
);

push @endpoints , (
    '127.0.0.1',
);

EOF

chmod 600  ~/.s3curl

rv=1
createBucket() {
    echo "create bucket $1"
    /tmp/s3curl/s3curl.pl --id=scality --createBucket -- http://127.0.0.1:$S3_DOCKER_PORT/$1 -w "%{http_code}" 2>/dev/null | grep 200
    rv=$?
    if [[ $rv -ne 0 ]]; then
        echo "put of bucket $1 failed"
    fi
}

put() {
    bucket=$1
    f=$2
    key=$3
    echo "putting $f to $bucket/$key ($4)"
    /tmp/s3curl/s3curl.pl --id=scality --put=$f -- http://127.0.0.1:$S3_DOCKER_PORT/$bucket/$key -w "%{http_code}" 2>/dev/null | grep 200
    rv=$?
    if [[ $rv -ne 0 ]]; then
        echo "putting $f to $bucket/$key ($4) failed"
        if [[ "$4" != "ok_to_fail" ]]; then
            exit 1
        fi
    fi
}

head() {
    bucket=$1
    key=$2
    echo "head $bucket/$key"
    /tmp/s3curl/s3curl.pl --id=scality --head -- http://127.0.0.1:$S3_DOCKER_PORT/$bucket/$key -w "%{http_code}" 2>/dev/null | grep 200
    rv=$?
    if [[ $rv -ne 0 ]]; then    
        echo "head of $bucket/$key failed"
        exit 1
    fi        
}

# Wait for bucket+file to get created
found=0
for i in `seq 20`; do
    createBucket "$S3_BUCKET"
    if [[ $rv -eq 0 ]]; then
        echo "bucket success"
        put "$S3_BUCKET" "$CSV" "`basename $CSV`" "ok_to_fail"
        if [[ $rv -eq 0 ]]; then
            echo "file success"
            found=1
            break
        fi
    fi
    sleep 15
done

if [ $found -eq 0 ]; then
    echo "Bucket never got created"
    docker logs s3server
    exit 1
fi

put "$S3_BUCKET" "$CSV1" "`basename $CSV1`"
put "$S3_BUCKET" "$CSV2" "grades/grades.csv"
put "$S3_BUCKET" "$AVRODATA1" "`basename $AVRODATA1`"
put "$S3_BUCKET" "$JSON1" "cartoondb/cartoon_table.json"
put "$S3_BUCKET" "$JSON2" "jsondata/json_datafile"
put "$S3_BUCKET" "$JSON3" "`basename $JSON3`"
put "$S3_BUCKET" "$PARQUET" "customer/customerfile"
put "$S3_BUCKET" "$PARQUET1" "store/storefile"
put "$S3_BUCKET" "$TXTFILE" "`basename $TXTFILE`"

head "$S3_BUCKET" "customer/customerfile"

if [ -f ~/.s3curl.bak.$$ ]; then
    mv ~/.s3curl.bak.$$ ~/.s3curl
fi

