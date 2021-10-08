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
package com.facebook.presto.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.facebook.presto.s3.util.SimpleServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class BucketObjectIteratorTest {

    private static final int PORT = 9999;

    private SimpleServer server;

    private AmazonS3 client;

    @BeforeClass
    public void beforeClass() {
        server = new SimpleServer(PORT);
        server.start();

        client = server.getClient();
    }

    @AfterClass
    public void afterClass() {
        server.stop();
    }


    @Test
    public void testPutGet() {
        server.putKey("bucket1", "abc", "abcdefg".getBytes(StandardCharsets.UTF_8));
        server.putKey("bucket1", "key2", "abcdefg".getBytes(StandardCharsets.UTF_8));

        GetObjectRequest request = new GetObjectRequest("bucket1", "key2").withRange(2);
        String s = readToString(client.getObject(request).getObjectContent());
        System.out.println(s);

        s = readToString(client.getObject("bucket1", "key2").getObjectContent());
        System.out.println(s);


        for (S3ObjectSummary s3ObjectSummary : client.listObjects("bucket1").getObjectSummaries()) {
            System.out.println(s3ObjectSummary.getKey());
        }

        for (S3ObjectSummary s3ObjectSummary : client.listObjects("bucket1", "ke").getObjectSummaries()) {
            System.out.println(s3ObjectSummary.getKey());
        }
    }

    private String readToString(InputStream inputStream) {
        int n;
        byte[] b = new byte[4096];
        StringBuilder sb = new StringBuilder();

        try {
            do {
                n = inputStream.read(b);
                if (n > 0) {
                    sb.append(new String(b, 0, n));
                }
            } while (n > 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return sb.toString();
    }

    @Test
    public void testIterator() {
        BucketObjectIterator iterator =
                new BucketObjectIterator(client, "bucket1", Collections.singletonList("key1"));


        iterator.hasNext();

        //client().getObject("bucket1", "key1");
        System.out.println("abc");
    }
}


