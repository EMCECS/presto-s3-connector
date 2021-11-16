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
 * Note: This file contains code from PrestoDb. Specifically PrestoS3InputStream is from the Hive Connector.
 * https://github.com/prestodb/presto/blob/7e4fe3d909a598d561b7e9b8b37c606a9df7241c/presto-hive/src/main/java/com/facebook/presto/hive/s3/PrestoS3FileSystem.java
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
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CSVInput;
import com.amazonaws.services.s3.model.CSVOutput;
import com.amazonaws.services.s3.model.CompressionType;
import com.amazonaws.services.s3.model.ExpressionType;
import com.amazonaws.services.s3.model.FileHeaderInfo;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.RequestProgress;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.ScanRange;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3Exception;
import com.emc.object.s3.bean.MetadataSearchDatatype;
import com.emc.object.s3.bean.MetadataSearchKey;
import com.emc.object.s3.bean.MetadataSearchList;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.facebook.airlift.log.Logger;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.units.DataSize;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.inject.Inject;

import java.net.URI;
import java.net.URISyntaxException;
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

public class S3AccessObject {
    private final S3ConnectorConfig s3ConnectorConfig;
    private static S3Client s3Client;
    private static String URL_PATTERN = "http://%s:%s/";
    private static final int HTTP_RANGE_NOT_SATISFIABLE = 416;
    private static final DataSize MAX_SKIP_SIZE = new DataSize(1, MEGABYTE);

    private AmazonS3 amazonS3Client;
    private static final Logger log = Logger.get(S3AccessObject.class);

    @Inject
    public S3AccessObject(S3ConnectorConfig s3ConnectorConfig) {
        log.info("create AmazonS3Client with " + s3ConnectorConfig.getMaxConnections() + " max connections");
        this.s3ConnectorConfig = requireNonNull(s3ConnectorConfig, "config is null");
        S3Config s3Config;
        try {
            URI s3Node = new URI(String.format(URL_PATTERN, s3ConnectorConfig.getRandomS3Node(), s3ConnectorConfig.getS3Port()));

            s3Config = new S3Config(s3Node);
            s3Config.withIdentity(s3ConnectorConfig.getS3UserKey()).withSecretKey(s3ConnectorConfig.getS3UserSecretKey());
            s3Client = new S3JerseyClient(s3Config);

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
            log.error("%s", e);
            throw new IllegalArgumentException(e);
        }
    }

    public AmazonS3 getS3Client() {
        return amazonS3Client;
    }

    public boolean bucketExists(String bucket) {
        return amazonS3Client.doesBucketExistV2(bucket);
    }

    public Long getObjectLength(String bucket, String key) {
        if (key.startsWith("/")) {
            key = key.replaceFirst("/", "");
        }
        Long length = amazonS3Client.getObjectMetadata(bucket, key).getContentLength();
        log.info("Object length for bucket " + bucket + " and object " + key + " = " + length);
        return length;
    }

    public PutObjectResult putObject(String table, String bucket, String key, InputStream stream, String file_format) {
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
        if (file_format.equalsIgnoreCase(JSON)) {
            finalStr = objName.concat(".json");
        } else if (file_format.equalsIgnoreCase(CSV)) {
            finalStr = objName.concat(".csv");
        }
        log.info("Put object " + objName + " into bucket " + bucket);
        PutObjectRequest request = new PutObjectRequest(bucket, objName, stream, metadata);
        return amazonS3Client.putObject(request);
    }

    public List<Bucket> listBuckets() {
        return amazonS3Client.listBuckets();
    }

    public List<Map<String, Object>> searchObjectMetadata(String bucket) {
        // metadata search API extension
        // https://github.com/EMCECS/presto-s3-connector/issues/57
        throw new UnsupportedOperationException("not yet implemented");
    }

    public JSONObject loadColumnsFromMetaDataSearchKeys(String bucketName) {
        JSONObject schema = new JSONObject();
        JSONArray columns = new JSONArray();
        MetadataSearchList listOfMetaData;
        try {
            // revisit when metadata search fully implemented with https://github.com/EMCECS/presto-s3-connector/issues/57
            listOfMetaData = new MetadataSearchList();
            for (MetadataSearchKey entry : listOfMetaData.getIndexableKeys()) {
                if (entry.getDatatype() == MetadataSearchDatatype.string) {
                    columns.put(new JSONObject().put("name", entry.getName()).put("type", "VARCHAR"));
                }
                if (entry.getDatatype() == MetadataSearchDatatype.integer) {
                    columns.put(new JSONObject().put("name", entry.getName()).put("type", "INTEGER"));
                }
                if (entry.getDatatype() == MetadataSearchDatatype.decimal) {
                    columns.put(new JSONObject().put("name", entry.getName()).put("type", "DOUBLE"));
                }
                if (entry.getDatatype() == MetadataSearchDatatype.datetime) {
                    columns.put(new JSONObject().put("name", entry.getName()).put("type", "VARCHAR"));
                }
            }
        } catch (S3Exception e) {
            // not all s3 compat. API support listBucketMetadataSearchKeys
        }
        schema.put("name", bucketName);
        schema.put("columns", columns);
        return schema;
    }

    public InputStream selectObjectContent(S3ObjectRange objectRange, String query, S3SelectProps props) {
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
        inputSerialization.setCompressionType(mapCompressionType(objectRange.getCompressionType()));

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

    private CompressionType mapCompressionType(String compressionCodecType) {
        switch (compressionCodecType) {
            case ".gz":
                return CompressionType.GZIP;
            case ".bz2":
                return CompressionType.BZIP2;
            default:
                return CompressionType.NONE;
        }
    }

    /**
     * This exception is for stopping retries for S3 calls that shouldn't be retried.
     * For example, "Caused by: com.amazonaws.services.s3.model.AmazonS3Exception: Forbidden (Service: Amazon S3; Status Code: 403 ..."
     */
    @VisibleForTesting
    static class UnrecoverableS3OperationException
            extends IOException {
        public UnrecoverableS3OperationException(String key, Throwable cause) {
            // append the path info to the message
            super(format("%s (key: %s)", cause, key), cause);
        }
    }

    public FSDataInputStream getFsDataInputStream(String bucket, String key, int bufferSize) {
        return new FSDataInputStream(
                new BufferedFSInputStream(
                        new PrestoS3InputStream(amazonS3Client, bucket, key),
                        bufferSize));
    }

    private static class PrestoS3InputStream
            extends FSInputStream {
        private final AmazonS3 s3;
        private final String host;
        private final String key;

        private final AtomicBoolean closed = new AtomicBoolean();

        private InputStream in;
        private long streamPosition;
        private long nextReadPosition;

        public PrestoS3InputStream(AmazonS3 s3, String host, String key) {
            this.s3 = requireNonNull(s3, "s3 is null");
            this.host = requireNonNull(host, "host is null");
            this.key = requireNonNull(key, "path is null");
        }

        @Override
        public void close() {
            closed.set(true);
            closeStream();
        }

        @Override
        public void seek(long pos)
                throws IOException {
            checkClosed();
            if (pos < 0) {
                throw new EOFException(NEGATIVE_SEEK);
            }

            // this allows a seek beyond the end of the stream but the next read will fail
            nextReadPosition = pos;
        }

        @Override
        public long getPos() {
            return nextReadPosition;
        }

        @Override
        public int read() {
            // This stream is wrapped with BufferedInputStream, so this method should never be called
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length)
                throws IOException {
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
                } catch (RuntimeException e) {
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
                } catch (Throwable t) {
                    abortStream(stream);
                    throw t;
                } finally {
                    stream.close();
                }
            } catch (Exception e) {
                throw propagate(e);
            }
        }

        @Override
        public int read(byte[] buffer, int offset, int length)
                throws IOException {
            checkClosed();
            int bytesRead;
            try {
                seekStream();
                try {
                    bytesRead = in.read(buffer, offset, length);
                } catch (Exception e) {
                    closeStream();
                    throw e;
                }

                if (bytesRead != -1) {
                    streamPosition += bytesRead;
                    nextReadPosition += bytesRead;
                }
                return bytesRead;
            } catch (Exception e) {
                throw propagate(e);
            }
        }

        @Override
        public boolean seekToNewSource(long targetPos) {
            return false;
        }

        private void seekStream()
                throws IOException {
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
                    } catch (IOException ignored) {
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
                throws IOException {
            if (in == null) {
                in = openStream(key, nextReadPosition);
                streamPosition = nextReadPosition;
            }
        }

        private InputStream openStream(String key, long start)
                throws IOException {
            try {
                GetObjectRequest request = new GetObjectRequest(host, key).withRange(start);
                return s3.getObject(request).getObjectContent();
            } catch (RuntimeException e) {
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

        private void closeStream() {
            if (in != null) {
                abortStream(in);
                in = null;
            }
        }

        private void checkClosed()
                throws IOException {
            if (closed.get()) {
                throw new IOException(STREAM_IS_CLOSED);
            }
        }

        private static void abortStream(InputStream in) {
            try {
                if (in instanceof S3ObjectInputStream) {
                    ((S3ObjectInputStream) in).abort();
                } else {
                    in.close();
                }
            } catch (IOException | AbortedException ignored) {
                // thrown if the current thread is in the interrupted state
            }
        }

        private static RuntimeException propagate(Exception e)
                throws IOException {
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

