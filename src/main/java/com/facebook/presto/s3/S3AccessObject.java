/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Note: This file contains changes from PrestoDb. Specifically the PrestoS3InputStream is inspired from the
 * Hive Connector.
 * https://github.com/prestodb/presto/blob/master/presto-hive/src/main/java/com/facebook/presto/hive/s3/PrestoS3FileSystem.java
 */
package com.facebook.presto.s3;

import com.amazonaws.AbortedException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.bean.MetadataSearchDatatype;
import com.emc.object.s3.bean.MetadataSearchKey;
import com.emc.object.s3.bean.MetadataSearchList;
import com.emc.object.s3.bean.S3Object;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.facebook.airlift.log.Logger;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.json.JSONArray;
import org.json.JSONObject;


import javax.inject.Inject;
import java.io.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.ConnectException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.s3.S3Const.CSV;
import static com.facebook.presto.s3.S3Const.JSON;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.net.HttpURLConnection.*;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.FSExceptionMessages.*;

public class S3AccessObject
{
    private final S3ConnectorConfig s3ConnectorConfig;
    private static S3Client s3Client;
    private static String URL_PATTERN = "http://%s:%s/";
    private static final int HTTP_RANGE_NOT_SATISFIABLE = 416;
    private static final DataSize MAX_SKIP_SIZE = new DataSize(1, MEGABYTE);

    private AmazonS3 amazonS3Client;
    private static final Logger log = Logger.get(S3AccessObject.class);

    @Inject
    public S3AccessObject(S3ConnectorConfig s3ConnectorConfig)
    {
        log.info("create AmazonS3Client with " + s3ConnectorConfig.getMaxConnections() + " max connections");
        this.s3ConnectorConfig = requireNonNull(s3ConnectorConfig, "config is null");
        S3Config s3Config;
        try {
            URI s3Node = new URI(String.format(URL_PATTERN, s3ConnectorConfig.getRandomS3Node(), s3ConnectorConfig.getS3Port()));

            s3Config =  new S3Config(s3Node);
            s3Config.withIdentity(s3ConnectorConfig.getS3UserKey()).withSecretKey(s3ConnectorConfig.getS3UserSecretKey());
            s3Client =  new S3JerseyClient(s3Config);

            amazonS3Client = AmazonS3Client.builder()
                    .withCredentials(
                            new AWSStaticCredentialsProvider(
                                    new BasicAWSCredentials(
                                            s3ConnectorConfig.getS3UserKey(), s3ConnectorConfig.getS3UserSecretKey())))
                    .withClientConfiguration(
                            new ClientConfiguration()
                                    .withProtocol(Protocol.HTTP)
                                    .withMaxConnections(s3ConnectorConfig.getMaxConnections())
                                    .withClientExecutionTimeout(s3ConnectorConfig.getS3ClientExecutionTimeout())
                                    .withSocketTimeout(s3ConnectorConfig.getS3SocketTimeout())
                                    .withConnectionTimeout(s3ConnectorConfig.getS3ConnectionTimeout()))
                    .withEndpointConfiguration(
                            new AwsClientBuilder.EndpointConfiguration(s3Node.toString(), null))
                    .enablePathStyleAccess()
                    .build();
        } catch (URISyntaxException e) {
            log.error("%s",e.getMessage());
            e.printStackTrace();
        }
    }

    public boolean bucketExists(String bucket) {
        return amazonS3Client.doesBucketExistV2(bucket);
    }

    public boolean objectOrDirExists (String bucket, String prefix) {
        // Just in case this wasn't confirmed prior
        if (!bucketExists(bucket)) {
            return false;
        }
        // Remove leading '/' from prefix
        if (prefix.charAt(0) == '/') {
            prefix = prefix.replaceFirst("/", "");
        }
        // Remove trailing '/' to test for object, but append if needed because prefix is a dir
        prefix = CharMatcher.is('/').trimTrailingFrom(prefix);
        log.info("Check existance of " + prefix + " in bucket " + bucket);
        try {
            ObjectMetadata metadata = amazonS3Client.getObjectMetadata(bucket, prefix);
            log.debug("Verified object " + prefix + " in bucket " + bucket + " of type "
                    + metadata.getContentType() + " of length " + metadata.getContentLength());
            return true;
        } catch (AmazonS3Exception e) {
            // Append a slash in case the object is actually a 'directory'
            try {
                ObjectMetadata metadata = amazonS3Client.getObjectMetadata(bucket, prefix + "/");
                log.debug("Verified " + prefix + " in bucket " + bucket + " of type "
                        + metadata.getContentType() + " of length " + metadata.getContentLength());
                return true;
            } catch (AmazonS3Exception e1) {
                try {
                    // Oddly, a directory created by Hadoop on S3 will fail the 'getObjectMetadata' call
                    // Last chance to check for existence, does listObjects work?
                    // At this point, object is either a directory, or is bogus
                    // If a directory, then we expect at least one object to be found in the directory
                    ListObjectsRequest request = new ListObjectsRequest();
                    request.setMaxKeys(1);
                    request.setBucketName(bucket);
                    request.setPrefix(prefix + "/");
                    ObjectListing listing = amazonS3Client.listObjects(request);
                    if (listing.getObjectSummaries().size() != 1) {
                        return false;
                    }
                    log.debug("Verified directory " + prefix + " in bucket " + bucket);
                    return true;
                } catch (Exception e2) {
                    return false;
                }
            }
        }
    }

    public Long getObjectLength(String bucket, String key)
    {
        if (key.startsWith("/")) {
            key = key.replaceFirst("/", "");
        }
        Long length = amazonS3Client.getObjectMetadata(bucket, key).getContentLength();
        log.info("Object length for bucket " + bucket + " and object " + key + " = " + length);
        return length;
    }

    public InputStream getObject(String bucket, String key) {
        return getObject(bucket, key, 0L /* start */);
    }

    public InputStream getObject(String bucket, String key, long start) {
        if (key.startsWith("/")) {
            key = key.replaceFirst("/", "");
        }
        GetObjectRequest request = new GetObjectRequest(bucket, key).withRange(start);
        return amazonS3Client.getObject(request).getObjectContent();
    }

    public PutObjectResult putObject (String table, String bucket, String key,  InputStream stream, String file_format) {
        ObjectMetadata metadata = new ObjectMetadata();
        try {
            metadata.setContentLength(stream.available());
        } catch (Exception e) {
            log.error("Error setting length: " + e);
        }

        if (!key.endsWith("/")) {
            key = key + "/";
        }
        if (key.startsWith("/")) {
            key = key.replaceFirst("/", "");
        }
        // ToDo: Don't assume csv suffix
        String objName = key + table + "_" + System.currentTimeMillis()
                + "_" + UUID.randomUUID();
        String finalStr = objName;
        if(file_format.equalsIgnoreCase(JSON)){
            finalStr = objName.concat(".json");
        } else if(file_format.equalsIgnoreCase(CSV)){
            finalStr = objName.concat(".csv");
        }
        log.info("Put object " + objName + " into bucket " + bucket);
        PutObjectRequest request = new PutObjectRequest(bucket, objName, stream, metadata);
        return amazonS3Client.putObject(request);
    }

    public List<String> listObjects(String bucket, String prefix){
        List<String> list = new Vector<>();
        String newPrefix = CharMatcher.is('/').trimTrailingFrom(prefix);
        if (newPrefix.startsWith("/")) {
            newPrefix = newPrefix.replaceFirst("/", "");
        }
        try {
            // If this is an object, getObjectMetadata (no trailing '/') will succeed

            amazonS3Client.getObjectMetadata(bucket, newPrefix);
            list.add(prefix);
            return list;
        } catch (AmazonS3Exception e1) {
            // Empty catch block
        }
        // So prefix must be a 'directory' - append a '/'
        ObjectListing listing = amazonS3Client.listObjects(bucket, newPrefix + "/");
        do {
            listing.getObjectSummaries().forEach(obj -> list.add(obj.getKey()));
            listing = listing.isTruncated() ? amazonS3Client.listNextBatchOfObjects(listing) : null;
        } while (listing != null);
        return list;

    }

    public List<String> listObjects(String bucket) {
        List<String> list = new Vector<>();
        ObjectListing listing = amazonS3Client.listObjects(bucket);
        do {
            listing.getObjectSummaries().forEach(obj -> list.add(obj.getKey()));
            listing = listing.isTruncated() ? amazonS3Client.listNextBatchOfObjects(listing) : null;
        } while (listing != null);
        return list;
    }

    public List<Bucket> listBuckets() {
        try {
            List<Bucket> bucketList = amazonS3Client.listBuckets();
            return bucketList;
        } catch (Exception e) {
            return Collections.EMPTY_LIST;
        }
    }

    public List<Map<String, Object>> listObjectMetadata(String bucket){
        MetadataSearchList listOfMetaDataNew;
        List<S3Object> objectList =  s3Client.listObjects(bucket).getObjects();
        List<Map<String, Object>> listOfMaps = new ArrayList<>();
        try {
            listOfMetaDataNew  = s3Client.listBucketMetadataSearchKeys(bucket);
            for(S3Object obj: objectList){
                Map<String,Object> map = new HashMap<>();
                for(MetadataSearchKey entry: listOfMetaDataNew.getIndexableKeys()){
                    if(entry.getName().toLowerCase().equals("lastmodified")){
                        map.put(entry.getName().toLowerCase(), obj.getLastModified());
                    }
                    if(entry.getName().toLowerCase().equals("size")){
                        map.put(entry.getName().toLowerCase(), obj.getSize());
                    }
                    if(entry.getName().toLowerCase().equals("owner")){
                        map.put(entry.getName().toLowerCase(), obj.getOwner().getDisplayName());
                    }
                    if(entry.getName().toLowerCase().equals("objectname")){
                        map.put(entry.getName().toLowerCase(), obj.getKey());
                    }
                    if(entry.getName().toLowerCase().equals("createtime")){
                        map.put(entry.getName().toLowerCase(), null);
                    }
                    if(entry.getName().toLowerCase().contains("x-amz-meta-")){
                        String[] splitArray = entry.getName().toLowerCase().split("-");
                        Map<String,String> userMetaDataMap = s3Client.getObjectMetadata(bucket, obj.getKey()).getUserMetadata();
                        map.put(entry.getName().toLowerCase(), userMetaDataMap.get(splitArray[splitArray.length -1].toLowerCase()));
                    }
                }
                listOfMaps.add(map);
            }
        } catch (S3Exception e){
            log.error("%s",e.getMessage());
        }
        return listOfMaps;

    }

    public JSONObject getMetaData(String bucketName){
        JSONObject schema =  new JSONObject();
        JSONArray columns = new JSONArray();
        MetadataSearchList listOfMetaData;
        try {
            listOfMetaData  = s3Client.listBucketMetadataSearchKeys(bucketName);
            for(MetadataSearchKey entry: listOfMetaData.getIndexableKeys()){
                if(entry.getDatatype() == MetadataSearchDatatype.string){
                    columns.put(new JSONObject().put("name", entry.getName()).put("type", "VARCHAR"));
                }
                if(entry.getDatatype() == MetadataSearchDatatype.integer){
                    columns.put(new JSONObject().put("name", entry.getName()).put("type", "INTEGER"));
                }
                if(entry.getDatatype() == MetadataSearchDatatype.decimal){
                    columns.put(new JSONObject().put("name", entry.getName()).put("type", "DOUBLE"));
                }
                if(entry.getDatatype() == MetadataSearchDatatype.datetime){
                    columns.put(new JSONObject().put("name", entry.getName()).put("type", "VARCHAR"));
                }
            }
        } catch (S3Exception e){
            log.error("%s",e.getMessage());
        }
        schema.put("name", bucketName);
        schema.put("columns", columns);
        return schema;
    }

    public InputStream selectObjectContent(S3ObjectRange objectRange, String query, S3SelectProps props)
    {
        SelectObjectContentRequest request = new SelectObjectContentRequest();
        request.setBucketName(objectRange.getBucket());
        request.setKey(objectRange.getKey());
        request.setExpression(query);
        request.setExpressionType(ExpressionType.SQL);
        request.setRequestProgress(new RequestProgress().withEnabled(false));

        InputSerialization inputSerialization = new InputSerialization();
        OutputSerialization outputSerialization = new OutputSerialization();
        request.setInputSerialization(inputSerialization);
        request.setOutputSerialization(outputSerialization);

        inputSerialization.setCsv(new CSVInput()
                .withFieldDelimiter(props.getFieldDelim())
                .withRecordDelimiter(props.getRecordDelim())
                .withFileHeaderInfo(props.getUseHeader()
                        ? FileHeaderInfo.USE
                        : FileHeaderInfo.IGNORE));

        outputSerialization.setCsv(new CSVOutput()
                .withFieldDelimiter(props.getFieldDelim())
                .withRecordDelimiter(props.getRecordDelim()));

        if (objectRange.getLength() > 0) {
            ScanRange scanRange = new ScanRange();
            scanRange.setStart(objectRange.getOffset());
            scanRange.setEnd(objectRange.getOffset() + objectRange.getLength());
            request.setScanRange(scanRange);
        }

        return amazonS3Client
                .selectObjectContent(request)
                .getPayload()
                .getRecordsInputStream();
    }

    /**
     * This exception is for stopping retries for S3 calls that shouldn't be retried.
     * For example, "Caused by: com.amazonaws.services.s3.model.AmazonS3Exception: Forbidden (Service: Amazon S3; Status Code: 403 ..."
     */
    @VisibleForTesting
    static class UnrecoverableS3OperationException
            extends IOException
    {
        public UnrecoverableS3OperationException(String key, Throwable cause)
        {
            // append the path info to the message
            super(format("%s (key: %s)", cause, key), cause);
        }
    }
    public FSDataInputStream getParquetObject(String bucket, String key, int bufferSize)
    {
        return new FSDataInputStream(
                new BufferedFSInputStream(
                        new PrestoS3InputStream(amazonS3Client, bucket, key),
                        bufferSize));
    }

    private static class PrestoS3InputStream
            extends FSInputStream
    {
        private final AmazonS3 s3;
        private final String host;
        private final String key;

        private final AtomicBoolean closed = new AtomicBoolean();

        private InputStream in;
        private long streamPosition;
        private long nextReadPosition;

        public PrestoS3InputStream(AmazonS3 s3, String host, String key)
        {
            this.s3 = requireNonNull(s3, "s3 is null");
            this.host = requireNonNull(host, "host is null");
            this.key = requireNonNull(key, "path is null");

        }

        @Override
        public void close()
        {
            closed.set(true);
            closeStream();
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length)
                throws IOException
        {
            checkClosed();
            if (position < 0) {
                throw new EOFException(NEGATIVE_SEEK);
            }
            checkPositionIndexes(offset, offset + length, buffer.length);
            if (length == 0) {
                return 0;
            }

            try {
                InputStream stream;
                try {
                    GetObjectRequest request = new GetObjectRequest(host, key)
                            .withRange(position, (position + length) - 1);
                    stream = s3.getObject(request).getObjectContent();
                }
                catch (RuntimeException e) {
                    if (e instanceof AmazonS3Exception) {
                        switch (((AmazonS3Exception) e).getStatusCode()) {
                            case HTTP_RANGE_NOT_SATISFIABLE:
                                throw new EOFException(CANNOT_SEEK_PAST_EOF);
                            case HTTP_FORBIDDEN:
                            case HTTP_NOT_FOUND:
                            case HTTP_BAD_REQUEST:
                                throw new UnrecoverableS3OperationException(key, e);
                        }
                    }
                    throw e;
                }

                try {
                    int read = 0;
                    while (read < length) {
                        int n = stream.read(buffer, offset + read, length - read);
                        if (n <= 0) {
                            if (read > 0) {
                                return read;
                            }
                            return -1;
                        }
                        read += n;
                    }
                    return read;
                }
                catch (Throwable t) {
                    abortStream(stream);
                    throw t;
                }
                finally {
                    stream.close();
                }
            }
            catch (Exception e) {
                throw propagate(e);
            }
        }

        @Override
        public void seek(long pos)
                throws IOException
        {
            checkClosed();
            if (pos < 0) {
                throw new EOFException(NEGATIVE_SEEK);
            }

            // this allows a seek beyond the end of the stream but the next read will fail
            nextReadPosition = pos;
        }

        @Override
        public long getPos()
        {
            return nextReadPosition;
        }

        @Override
        public int read()
        {
            // This stream is wrapped with BufferedInputStream, so this method should never be called
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(byte[] buffer, int offset, int length)
                throws IOException
        {
            checkClosed();
            int bytesRead;
            try {
                seekStream();
                try {
                    bytesRead = in.read(buffer, offset, length);
                }
                catch (Exception e) {
                    closeStream();
                    throw e;
                }

                if (bytesRead != -1) {
                    streamPosition += bytesRead;
                    nextReadPosition += bytesRead;
                }
                return bytesRead;
            }
            catch (Exception e) {
                throw propagate(e);
            }
        }

        @Override
        public boolean seekToNewSource(long targetPos)
        {
            return false;
        }

        private void seekStream()
                throws IOException
        {
            if ((in != null) && (nextReadPosition == streamPosition)) {
                // already at specified position
                return;
            }

            if ((in != null) && (nextReadPosition > streamPosition)) {
                // seeking forwards
                long skip = nextReadPosition - streamPosition;
                if (skip <= max(in.available(), MAX_SKIP_SIZE.toBytes())) {
                    // already buffered or seek is small enough
                    try {
                        if (in.skip(skip) == skip) {
                            streamPosition = nextReadPosition;
                            return;
                        }
                    }
                    catch (IOException ignored) {
                        // will retry by re-opening the stream
                    }
                }
            }

            // close the stream and open at desired position
            streamPosition = nextReadPosition;
            closeStream();
            openStream();
        }

        private void openStream()
                throws IOException
        {
            if (in == null) {
                in = openStream(key, nextReadPosition);
                streamPosition = nextReadPosition;
            }
        }

        private InputStream openStream(String key, long start)
                throws IOException
        {
            try {
                GetObjectRequest request = new GetObjectRequest(host, key).withRange(start);
                return s3.getObject(request).getObjectContent();
            }
            catch (RuntimeException e) {
                if (e instanceof AmazonS3Exception) {
                    switch (((AmazonS3Exception) e).getStatusCode()) {
                        case HTTP_RANGE_NOT_SATISFIABLE:
                            // ignore request for start past end of object
                            return new ByteArrayInputStream(new byte[0]);
                        case HTTP_FORBIDDEN:
                        case HTTP_NOT_FOUND:
                        case HTTP_BAD_REQUEST:
                            throw new UnrecoverableS3OperationException(key, e);
                    }
                }
                throw e;
            }
        }

        private void closeStream()
        {
            if (in != null) {
                abortStream(in);
                in = null;
            }
        }

        private void checkClosed()
                throws IOException
        {
            if (closed.get()) {
                throw new IOException(STREAM_IS_CLOSED);
            }
        }

        private static void abortStream(InputStream in)
        {
            try {
                if (in instanceof S3ObjectInputStream) {
                    ((S3ObjectInputStream) in).abort();
                }
                else {
                    in.close();
                }
            }
            catch (IOException | AbortedException ignored) {
                // thrown if the current thread is in the interrupted state
            }
        }

        private static RuntimeException propagate(Exception e)
                throws IOException
        {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException();
            }
            throwIfInstanceOf(e, IOException.class);
            throwIfUnchecked(e);
            throw new IOException(e);
        }
    }
}

