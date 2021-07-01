#!/bin/bash

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
export AVRODATA1=$(readlink --canonicalize $SCRIPTDIR/../resources/avro_datafile)
export TXTFILE=$(readlink --canonicalize $SCRIPTDIR/../resources/datafile.txt)
export PARQUET=$(readlink --canonicalize $SCRIPTDIR/../resources/customerfile)
export PARQUET1=$(readlink --canonicalize $SCRIPTDIR/../resources/storefile)
export CSVDIR=$(dirname $CSV)

if [ ! -f /tmp/github.action.s3 ]; then
    echo "Starting s3 docker container"
    docker pull scality/s3server
    docker run -d --name s3server -p 8000:8000 -p 9990:9990 -p 9991:9991 scality/s3server || exit 1
else
    rm -f /tmp/github.action.s3
fi

echo "TEST123A"

found=0
for i in 1 2 3 4 5 6 7 8 9 10; do
    curl -s 127.0.0.1:8000 >/dev/null
    if [ $? -eq 0 ]; then
        found=1
        break;
    fi
    sleep 3
done
echo "TEST123B"

if [ $found -eq 0 ]; then
    echo "Image run failed: docker run -d --name s3server -p $S3_DOCKER_PORT:$S3_DOCKER_PORT scality/s3server"
    exit 1
fi
echo "TEST123C"

if [ ! -f /tmp/s3curl/s3curl.pl ]; then
    DIR=`pwd`
    cd  /tmp
    git clone https://github.com/EMCECS/s3curl.git
    chmod +x /tmp/s3curl/s3curl.pl
    cd $DIR
fi
echo "TEST123D"

if [ -f ~/.s3curl ]; then
    mv ~/.s3curl ~/.s3curl.bak.$$
fi

cat > ~/.s3curl << EOF
%awsSecretAccessKeys = (
    # personal account
    # personal is a [friendly-name] . It can be named anything & is used in given s3curl commands.
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

echo "TEST123E"

# Sleep a bit for the s3 server to become ready

echo "Creating bucket $S3_BUCKET"
/tmp/s3curl/s3curl.pl --id=scality --createBucket -- http://127.0.0.1:$S3_DOCKER_PORT/$S3_BUCKET
sleep 2
echo "Copy $CSV to $S3_BUCKET"
/tmp/s3curl/s3curl.pl --id=scality --put=$CSV -- http://127.0.0.1:$S3_DOCKER_PORT/$S3_BUCKET/`basename $CSV`
echo "Copy $CSV1 to $S3_BUCKET"
/tmp/s3curl/s3curl.pl --id=scality --put=$CSV1 -- http://127.0.0.1:$S3_DOCKER_PORT/$S3_BUCKET/`basename $CSV1`
echo "Copy $CSV2 to $S3_BUCKET/grades"
/tmp/s3curl/s3curl.pl --id=scality --put=$CSV2 -- http://127.0.0.1:$S3_DOCKER_PORT/$S3_BUCKET/grades/grades.csv
echo "Copy $AVRODATA1 to $S3_BUCKET"
/tmp/s3curl/s3curl.pl --id=scality --put=$AVRODATA1 -- http://127.0.0.1:$S3_DOCKER_PORT/$S3_BUCKET/`basename $AVRODATA1`
echo "Copy $JSON1 to $S3_BUCKET"
/tmp/s3curl/s3curl.pl --id=scality --put=$JSON1 -- http://127.0.0.1:$S3_DOCKER_PORT/$S3_BUCKET/cartoondb/cartoon_table.json
echo "Copy $JSON2 to $S3_BUCKET"
/tmp/s3curl/s3curl.pl --id=scality --put=$JSON2 -- http://127.0.0.1:$S3_DOCKER_PORT/$S3_BUCKET/jsondata/json_datafile
echo "Copy $PARQUET to $S3_BUCKET"
/tmp/s3curl/s3curl.pl --id=scality --put=$PARQUET -- http://127.0.0.1:$S3_DOCKER_PORT/$S3_BUCKET/customer/customerfile
echo "Copy $PARQUET1 to $S3_BUCKET"
/tmp/s3curl/s3curl.pl --id=scality --put=$PARQUET1 -- http://127.0.0.1:$S3_DOCKER_PORT/$S3_BUCKET/store/storefile
echo "Copy $TXTFILE to $S3_BUCKET"
/tmp/s3curl/s3curl.pl --id=scality --put=$TXTFILE -- http://127.0.0.1:$S3_DOCKER_PORT/$S3_BUCKET/`basename $TXTFILE`
echo "Get $S3_BUCKET contents"
/tmp/s3curl/s3curl.pl --id=scality -- http://127.0.0.1:$S3_DOCKER_PORT/$S3_BUCKET/

if [ -f ~/.s3curl.bak.$$ ]; then
    mv ~/.s3curl.bak.$$ ~/.s3curl
fi

echo "TEST123F"

docker logs s3server
