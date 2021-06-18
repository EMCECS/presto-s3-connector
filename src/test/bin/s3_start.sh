#!/bin/sh

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

echo "Starting s3 docker container"
docker pull scality/s3server
docker run -d --name s3server -p $S3_DOCKER_PORT:$S3_DOCKER_PORT scality/s3server || exit 1

aws configure --profile s3connectortest << EOF > /dev/null
$S3_ACCESS_KEY
$S3_SECRET_KEY
us-east-1
json
EOF

# Sleep a bit for the s3 server to become ready
sleep 5
docker ps

echo "Creating bucket $S3_BUCKET"
aws --profile s3connectortest --endpoint-url http://localhost:$S3_DOCKER_PORT s3 mb s3://$S3_BUCKET/
echo "Copy $CSV to $S3_BUCKET"
aws --profile s3connectortest --endpoint-url http://localhost:$S3_DOCKER_PORT s3 cp $CSV s3://$S3_BUCKET/
echo "Copy $CSV1 to $S3_BUCKET"
aws --profile s3connectortest --endpoint-url http://localhost:$S3_DOCKER_PORT s3 cp $CSV1 s3://$S3_BUCKET/
echo "Copy $CSV2 to $S3_BUCKET/grades"
aws --profile s3connectortest --endpoint-url http://localhost:$S3_DOCKER_PORT s3 cp $CSV2 s3://$S3_BUCKET/grades/grades.csv
echo "Copy $AVRODATA1 to $S3_BUCKET"
aws --profile s3connectortest --endpoint-url http://localhost:$S3_DOCKER_PORT s3 cp $AVRODATA1 s3://$S3_BUCKET/
echo "Copy $JSON1 to $S3_BUCKET"
aws --profile s3connectortest --endpoint-url http://localhost:$S3_DOCKER_PORT s3 cp $JSON1 s3://$S3_BUCKET/cartoondb/cartoon_table.json
echo "Copy $JSON2 to $S3_BUCKET"
aws --profile s3connectortest --endpoint-url http://localhost:$S3_DOCKER_PORT s3 cp $JSON2 s3://$S3_BUCKET/jsondata/json_datafile
echo "Copy $PARQUET to $S3_BUCKET"
aws --profile s3connectortest --endpoint-url http://localhost:$S3_DOCKER_PORT s3 cp $PARQUET s3://$S3_BUCKET/customer/customerfile
echo "Copy $PARQUET1 to $S3_BUCKET"
aws --profile s3connectortest --endpoint-url http://localhost:$S3_DOCKER_PORT s3 cp $PARQUET1 s3://$S3_BUCKET/store/storefile
echo "Copy $TXTFILE to $S3_BUCKET"
aws --profile s3connectortest --endpoint-url http://localhost:$S3_DOCKER_PORT s3 cp $TXTFILE s3://$S3_BUCKET/
