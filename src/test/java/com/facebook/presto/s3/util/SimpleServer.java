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
import com.google.common.base.Preconditions;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import java.util.*;

import static com.facebook.presto.s3.util.S3ErrorUtil.errorResponse;

@Path("/")
public class SimpleServer extends Application {

    private final Set<Object> singletons = new HashSet<>();

    private final int port;

    private Server server;

    public SimpleServer(int port) {
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
        server.setConnectors(new Connector[]{connector});


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
            for (S3Data s3Data : data.get(bucket)) {
                if (s3Data.key.equalsIgnoreCase(key)) {
                    return s3Data;
                }
            }
            return null;
        }
    }

    private S3DataStore dataStore = new S3DataStore();

    public void putKey(String bucket, String key, File f) {
        dataStore.addKey(bucket, key, f);
    }

    public void putKey(String bucket, String key, byte[] bytes) {
        dataStore.addKey(bucket, key, bytes);
    }

    @HEAD
    @Path("{bucket}/{key}")
    public Response getMetadata(@Context HttpServletResponse response,
                                @PathParam("bucket") String bucket,
                                @PathParam("key") String key) {
        S3Data data = dataStore.getKey(bucket, key);
        if (data == null) {
            return errorResponse(404, "NoSuchKey", "Cannot find key");
        } else {
            response.setContentLength(data.size());
            return Response.ok().build();
        }
    }

    @GET
    @Path("{bucket}/{key}")
    public Response getKey(@Context HttpServletRequest request,
                           @Context HttpServletResponse response,
                           @PathParam("bucket") String bucket,
                           @PathParam("key") String key) {
        final S3Data data = dataStore.getKey(bucket, key);
        if (data == null) {
            return errorResponse(404, "NoSuchKey", "Cannot find key");
        }

        int offset = 0;
        int len = Integer.MAX_VALUE;

        String range = request.getHeader("Range");
        if (range != null) {
            range = range.replace("bytes=", "");
            String[] parts = range.split("-");
            if (range.startsWith("-")) {
                // last N bytes
                offset = data.size() - Integer.parseInt(parts[0]);
                Preconditions.checkState(offset >= 0);
            } else {
                offset = Integer.parseInt(parts[0]);
                if (parts.length == 2) {
                    long end = Long.parseLong(parts[1]);
                    len = (int) Math.min(Integer.MAX_VALUE, end) - offset;
                    Preconditions.checkState(len >= 0);
                }
            }
        }

        int finalOffset = offset;
        int finalLen = len;

        return Response.ok((StreamingOutput) output ->
                data.writeTo(output, finalOffset, finalLen)).build();
    }

    @GET
    @Path("{bucket}/")
    public Response list(@PathParam("bucket") String bucket,
                         @QueryParam("prefix") String prefix,
                         @QueryParam("marker") String marker,
                         @QueryParam("max-keys") @DefaultValue("100") int maxKeys) {

        List<S3Data> data = dataStore.getBucket(bucket);
        if (data == null) {
            return errorResponse(404, "NoSuchBucket", "Cannot find bucket");
        }

        StringBuilder sb = new StringBuilder();
        sb.append("<ListBucketResult>");
        sb.append("<IsTruncated>").append("false").append("</IsTruncated>");
        sb.append("<Name>").append(bucket).append("</Name>");
        sb.append("<MaxKeys>").append(maxKeys).append("</MaxKeys>");
        sb.append("<Prefix>").append(prefix == null ? "" : prefix).append("</Prefix>");
        sb.append("<Marker>").append(marker == null ? "" : marker).append("</Marker>");

        for (S3Data key : data) {
            if (prefix == null || key.key.startsWith(prefix)) {
                addKey(sb, key);
            }
        }
        sb.append("</ListBucketResult>");

        return Response.ok(sb.toString()).build();
    }

    StringBuilder addKey(StringBuilder sb, S3Data key) {
        sb.append("<Contents>").append("\n");
        sb.append("<Key>").append(key.key).append("</Key>").append("\n");
        sb.append("<Size>").append(key.size()).append("</Size>").append("\n");
        sb.append("<ETag>").append("abcdefg").append("</ETag>").append("\n");
        sb.append("<LastModified>").append("2021-10-01T12:00:00.000Z").append("</LastModified>").append("\n");
        sb.append("<StorageClass>").append("tier2").append("</StorageClass>").append("\n");
        sb.append("<Owner>").append("\n")
                .append("<ID>").append("user1").append("</ID>")
                .append("<DisplayName>").append("user1").append("</DisplayName>")
                .append("</Owner>").append("\n");
        sb.append("</Contents>").append("\n");
        return sb;
    }
}
