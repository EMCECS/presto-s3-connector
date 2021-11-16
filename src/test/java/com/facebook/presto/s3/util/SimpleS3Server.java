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
 */
package com.facebook.presto.s3.util;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.emc.object.s3.bean.MetadataSearchList;
import com.facebook.presto.s3.Pair;
import com.google.common.base.Preconditions;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.http.impl.io.ChunkedInputStream;
import org.apache.http.impl.io.HttpTransportMetricsImpl;
import org.apache.http.impl.io.SessionInputBufferImpl;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.net.URI;

import static com.facebook.presto.s3.util.S3ErrorUtil.errorResponse;

@Path("/")
public class SimpleS3Server extends Application {

    private final Set<Object> singletons = new HashSet<>();

    private final int port;

    private Server server;

    public SimpleS3Server(int port) {
        this.port = port;
    }

    @Override
    public Set<Object> getSingletons() {
        return singletons;
    }

    public synchronized void start() {
        if (server != null) {
            return;
        }

        singletons.clear();
        singletons.add(this);

        server = new Server();

        ServerConnector connector = new ServerConnector(server);
        connector.setHost("127.0.0.1");
        connector.setPort(port);
        server.setConnectors(new Connector[] {connector});

        ServletContextHandler contextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        contextHandler.addServlet(new ServletHolder(new ServletContainer(ResourceConfig.forApplication(this))), "/*");
        server.setHandler(contextHandler);

        try {
            server.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void stop() {
        if (server == null) {
            return;
        }
        try {
            server.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            server = null;
        }
    }

    public AmazonS3 getClient() {
        return AmazonS3Client.builder()
                             .withClientConfiguration(
                                     new ClientConfiguration()
                                             .withProtocol(Protocol.HTTP))
                             .withEndpointConfiguration(
                                     new AwsClientBuilder.EndpointConfiguration("http://127.0.0.1:" + port, null))
                             .enablePathStyleAccess()
                             .build();
    }

    class S3Data {
        String key;
        byte[] bytes;
        File f;

        public S3Data(String key, File f) {
            this.key = key;
            this.f = f;
        }

        public S3Data(String key, byte[] bytes) {
            this.key = key;
            this.bytes = bytes;
        }

        public int size() {
            return bytes != null ? bytes.length : (int) f.length();
        }

        public void writeTo(OutputStream output, int off, int len) throws IOException {
            SeekableStream inputStream;
            if (f != null) {
                inputStream = new SeekableFileStream(f);
            } else {
                inputStream = new SeekableByteArrayInputStream(bytes);
            }

            inputStream.seek(off);

            byte[] buf = new byte[4096];
            int n;
            int remaining = len;
            do {
                n = inputStream.read(buf, 0, Math.min(remaining, buf.length));
                if (n > 0) {
                    output.write(buf, 0, n);
                    remaining -= n;
                }
            } while (n > 0 && remaining > 0);
        }
    }

    class S3DataStore {
        Map<String, List<S3Data>> data = new HashMap<>();

        public S3DataStore addBucket(String bucket) {
            if (!data.containsKey(bucket)) {
                data.put(bucket, new ArrayList<>());
            }
            return this;
        }

        public S3DataStore addKey(String bucket, String key, File f) {
            addBucket(bucket);
            data.get(bucket).add(new S3Data(key, f));
            return this;
        }

        public S3DataStore addKey(String bucket, String key, byte[] b) {
            addBucket(bucket);
            data.get(bucket).add(new S3Data(key, b));
            return this;
        }

        public List<S3Data> getBucket(String bucket) {
            return data.get(bucket);
        }

        public S3Data getKey(String bucket, String key) {
            if (!data.containsKey(bucket)) {
                return null;
            }

            for (S3Data s3Data : data.get(bucket)) {
                if (s3Data.key.equalsIgnoreCase(key)) {
                    return s3Data;
                }
            }
            return null;
        }

        public Set<String> getBuckets() {
            return data.keySet();
        }
    }

    private final S3DataStore dataStore = new S3DataStore();

    public void putKey(String bucket, String key, File f) {
        dataStore.addKey(bucket, key, f);
    }

    public void putKey(String bucket, String key, byte[] bytes) {
        dataStore.addKey(bucket, key, bytes);
    }

    @PUT
    @Path("{bucket}/{key: .+}")
    public Response putKey(@Context HttpServletRequest request,
                           @PathParam("bucket") String bucket,
                           @PathParam("key") String key) {

        ByteArrayOutputStream os = new ByteArrayOutputStream();

        try {
            SessionInputBufferImpl sessionInputBuffer =
                    new SessionInputBufferImpl(new HttpTransportMetricsImpl(), 65536);
            sessionInputBuffer.bind(request.getInputStream());
            InputStream inputStream = new ChunkedInputStream(sessionInputBuffer);

            int n;
            byte[] b = new byte[4096];

            do {
                n = inputStream.read(b);
                if (n > 0) {
                    os.write(b, 0, n);
                }
            } while (n > 0);
        } catch (IOException e) {
            return Response.serverError().build();
        }

        S3Data data = dataStore.getKey(bucket, key);

        if (data != null) {
            data.bytes = os.toByteArray();
            data.f = null;
            return Response.ok().build();
        } else {
            dataStore.addKey(bucket, key, os.toByteArray());
            URI location = URI.create("http://127.0.0.1:" + port + "/" + bucket + "/" + key);
            return Response.created(location).build();
        }
    }

    public void putBucket(String bucket) {
        dataStore.addKey(bucket, "", (byte[]) null);
    }

    Pair<Integer, Integer> parseRange(S3Data data, String range) {
        int offset = 0;
        int len = data.size();

        if (range == null) {
            return new Pair<>(offset, len);
        }

        range = range.replace("bytes=", "");
        String[] parts = range.split("-");
        if (range.startsWith("-")) {
            // last N bytes
            offset = data.size() - Integer.parseInt(parts[0]);
            len = Integer.parseInt(parts[0]);
            Preconditions.checkState(offset >= 0);
        } else {
            offset = Integer.parseInt(parts[0]);
            if (parts.length == 2) {
                long end = Long.parseLong(parts[1]);
                len = (int) Math.min(Integer.MAX_VALUE - 1, end) - offset + 1;
                Preconditions.checkState(len >= 0);
            }
        }

        return new Pair<>(offset, len);
    }

    @GET
    public Response listBuckets() {
        StringBuilder sb = new StringBuilder();
        sb.append("<ListAllMyBucketsResult>");
        sb.append("<Buckets>");
        for (String bucket : dataStore.getBuckets()) {
            sb.append("<Bucket>");
            sb.append("<CreationDate>").append("2021-10-01T12:00:00.000Z").append("</CreationDate>");
            sb.append("<Name>").append(bucket).append("</Name>");
            sb.append("</Bucket>");
        }
        sb.append("</Buckets>");
        sb.append("<Owner>")
          .append("<ID>").append("user1").append("</ID>")
          .append("<DisplayName>").append("user1").append("</DisplayName>")
          .append("</Owner>");
        sb.append("</ListAllMyBucketsResult>");
        return Response.ok(sb.toString()).build();
    }

    @HEAD
    @Path("{bucket}/{key: .+}")
    public Response getMetadata(@Context HttpServletRequest request,
                                @Context HttpServletResponse response,
                                @PathParam("bucket") String bucket,
                                @PathParam("key") String key) {
        if (dataStore.getBucket(bucket) == null) {
            return errorResponse(404, "NoSuchBucket", "Cannot find bucket");
        }

        S3Data data = dataStore.getKey(bucket, key);
        if (data == null) {
            return errorResponse(404, "NoSuchKey", "Cannot find key");
        } else {
            Pair<Integer, Integer> range = parseRange(data, request.getHeader("Range"));
            response.setContentLength(range.getRight());
            return Response.ok().build();
        }
    }

    @GET
    @Path("{bucket}/{key: .+}")
    public Response getKey(@Context HttpServletRequest request,
                           @Context HttpServletResponse response,
                           @PathParam("bucket") String bucket,
                           @PathParam("key") String key) {
        if (dataStore.getBucket(bucket) == null) {
            return errorResponse(404, "NoSuchBucket", "Cannot find bucket");
        }

        final S3Data data = dataStore.getKey(bucket, key);
        if (data == null) {
            return errorResponse(404, "NoSuchKey", "Cannot find key");
        }

        final Pair<Integer, Integer> range =
                parseRange(data, request.getHeader("Range"));

        return Response.ok((StreamingOutput) output ->
                data.writeTo(output, range.getLeft(), range.getRight())).build();
    }

    @GET
    @Path("{bucket}")
    public Response list(@Context HttpServletRequest request,
                         @PathParam("bucket") String bucket,
                         @QueryParam("prefix") String prefix,
                         @QueryParam("marker") String marker,
                         @QueryParam("max-keys") @DefaultValue("100") int maxKeys) {

        List<S3Data> data = dataStore.getBucket(bucket);
        if (data == null) {
            return errorResponse(404, "NoSuchBucket", "Cannot find bucket");
        }

        Object entity = request.getParameterMap().containsKey("searchmetadata")
                ? searchMdKeys(bucket)
                : listObjects(data, bucket, maxKeys, prefix, marker);

        return Response.ok(entity, MediaType.APPLICATION_XML_TYPE).build();
    }

    private Object searchMdKeys(String bucket) {
        return new MetadataSearchList();
    }

    private Object listObjects(List<S3Data> data, String bucket, int maxKeys, String prefix, String marker) {
        StringBuilder sb = new StringBuilder();
        sb.append("<ListBucketResult>");
        sb.append("<IsTruncated>").append("false").append("</IsTruncated>");
        sb.append("<Name>").append(bucket).append("</Name>");
        sb.append("<MaxKeys>").append(maxKeys).append("</MaxKeys>");
        sb.append("<Prefix>").append(prefix == null ? "" : prefix).append("</Prefix>");
        sb.append("<Marker>").append(marker == null ? "" : marker).append("</Marker>");

        data.sort(Comparator.comparing(s3Data -> s3Data.key));

        for (S3Data key : data) {
            if (!key.key.isEmpty() &&
                    (prefix == null || key.key.startsWith(prefix))) {
                addToListResults(sb, key);
            }
        }
        sb.append("</ListBucketResult>");
        return sb.toString();
    }

    StringBuilder addToListResults(StringBuilder sb, S3Data key) {
        sb.append("<Contents>");
        sb.append("<Key>").append(key.key).append("</Key>");
        sb.append("<Size>").append(key.size()).append("</Size>");
        sb.append("<ETag>").append("abcdefg").append("</ETag>");
        sb.append("<LastModified>").append("2021-10-01T12:00:00.000Z").append("</LastModified>");
        sb.append("<StorageClass>").append("STANDARD").append("</StorageClass>");
        sb.append("<Owner>")
          .append("<ID>").append("user1").append("</ID>")
          .append("<DisplayName>").append("user1").append("</DisplayName>")
          .append("</Owner>");
        sb.append("</Contents>");
        return sb;
    }
}
