#!/bin/sh

export MINIO_DOCKER_NAME=test-minio
export MINIO_ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
export MINIO_SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
export MINIO_BUCKET=testbucket
SCRIPT=$(readlink -f $0)
SCRIPTDIR=$(dirname $SCRIPT)
export CSV=$(readlink --canonicalize $SCRIPTDIR/../resources/medical.csv)
export CSV1=$(readlink --canonicalize $SCRIPTDIR/../resources/names.csv)
export CSV2=$(readlink --canonicalize $SCRIPTDIR/../resources/grades.csv)
export JSON1=$(readlink --canonicalize $SCRIPTDIR/../resources/json_datafile)
export JSON2=$(readlink --canonicalize $SCRIPTDIR/../resources/json_data_file)
export AVRODATA1=$(readlink --canonicalize $SCRIPTDIR/../resources/avro_datafile)
export TXTFILE=$(readlink --canonicalize $SCRIPTDIR/../resources/datafile.txt)
export PARQUET=$(readlink --canonicalize $SCRIPTDIR/../resources/customerfile)
export PARQUET1=$(readlink --canonicalize $SCRIPTDIR/../resources/storefile)
export CSVDIR=$(dirname $CSV)

echo "Starting minio/minio docker container"
docker pull minio/minio
docker run -d -p 9000:9000 -e MINIO_ACCESS_KEY=$MINIO_ACCESS_KEY -e MINIO_SECRET_KEY=$MINIO_SECRET_KEY minio/minio server /data || exit 1

echo "Creating bucket $MINIO_BUCKET"
aws --endpoint-url http://localhost:9000 s3 mb s3://$MINIO_BUCKET
echo "Copy $CSV to $MINIO_BUCKET"
aws --endpoint-url http://localhost:9000 s3 cp $CSV s3://$MINIO_BUCKET
echo "Copy $CSV1 to $MINIO_BUCKET"
aws --endpoint-url http://localhost:9000 s3 cp $CSV1 s3://$MINIO_BUCKET
echo "Copy $CSV2 to $MINIO_BUCKET/grades"
aws --endpoint-url http://localhost:9000 s3 cp $CSV2 s3://$MINIO_BUCKET/grades/grades.csv
echo "Copy $AVRODATA1 to $MINIO_BUCKET"
aws --endpoint-url http://localhost:9000 s3 cp $AVRODATA1 s3://$MINIO_BUCKET
echo "Copy $JSON1 to $MINIO_BUCKET"
aws --endpoint-url http://localhost:9000 s3 cp $JSON1 s3://$MINIO_BUCKET/cartoondb/cartoon_table.json
echo "Copy $JSON2 to $MINIO_BUCKET"
aws --endpoint-url http://localhost:9000 s3 cp $JSON2 s3://$MINIO_BUCKET/jsondata/json_data_file
echo "Copy $PARQUET to $MINIO_BUCKET"
aws --endpoint-url http://localhost:9000 s3 cp $PARQUET s3://$MINIO_BUCKET/customer/customerfile
echo "Copy $PARQUET1 to $MINIO_BUCKET"
aws --endpoint-url http://localhost:9000 s3 cp $PARQUET1 s3://$MINIO_BUCKET/store/storefile
echo "Copy $TXTFILE to $MINIO_BUCKET"
aws --endpoint-url http://localhost:9000 s3 cp $TXTFILE s3://$MINIO_BUCKET
