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
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.facebook.presto.s3.util.SimpleS3Server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class BucketObjectIteratorTest {

    private static final int PORT = 9999;

    private SimpleS3Server server;

    private AmazonS3 client;

    @BeforeClass
    public void beforeClass() {
        server = new SimpleS3Server(PORT);
        server.start();

        client = server.getClient();
    }

    @AfterClass
    public void afterClass() {
        server.stop();
    }

    @Test
    public void testBucketNotFound() {
        BucketObjectIterator iterator =
                new BucketObjectIterator(client, "not-found-bucket", Collections.singletonList("key1"));
        boolean ex = false;
        try {
            iterator.hasNext();
        } catch (AmazonS3Exception e) {
            ex = e.getStatusCode() == 404;
        }
        assertTrue(ex);
    }

    @Test
    public void testSingleKey() {
        String bucket = "test-single-key";

        server.putKey(bucket, "key1", new byte[] {'a'});

        BucketObjectIterator iterator =
                new BucketObjectIterator(client, bucket, Collections.singletonList("key1"));
        assertTrue(iterator.hasNext());
        assertEquals(iterator.next().getKey(), "key1");
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testPrefix() {
        String bucket = "test-prefix";
        server.putKey(bucket, "2021-10-11/hour00", new byte[] {'a'});
        server.putKey(bucket, "2021-10-11/hour01", new byte[] {'a'});
        server.putKey(bucket, "2021-10-11/hour02", new byte[] {'a'});
        server.putKey(bucket, "2021-10-11/hour03", new byte[] {'a'});

        BucketObjectIterator iterator =
                new BucketObjectIterator(client, bucket, Collections.singletonList("2021-10-11"));
        int i = 0;
        while (iterator.hasNext()) {
            assertEquals(iterator.next().getKey(), "2021-10-11/hour0" + i++);
        }
        assertEquals(i, 4);
    }

    @Test
    public void testMix() {
        String bucket = "test-mix";

        server.putKey(bucket, "key1", new byte[] {'a'});
        server.putKey(bucket, "2021-10-11/hour00", new byte[] {'a'});
        server.putKey(bucket, "2021-10-11/hour01", new byte[] {'a'});

        List<String> subObjects = new ArrayList<>();
        subObjects.add("key1");
        subObjects.add("2021-10-11");

        BucketObjectIterator iterator =
                new BucketObjectIterator(client, bucket, subObjects);

        assertTrue(iterator.hasNext());
        assertEquals(iterator.next().getKey(), "key1");

        assertTrue(iterator.hasNext());
        assertEquals(iterator.next().getKey(), "2021-10-11/hour00");

        assertTrue(iterator.hasNext());
        assertEquals(iterator.next().getKey(), "2021-10-11/hour01");

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testAll() {
        String bucket = "test-all";

        int count = 9;
        for (int i = 0; i < count; i++) {
            server.putKey(bucket, "key" + i, new byte[] {'a'});
        }

        // single, empty object in list means all objects
        BucketObjectIterator iterator =
                new BucketObjectIterator(client, bucket, Collections.singletonList(""));
        int i = 0;
        while (iterator.hasNext()) {
            assertEquals(iterator.next().getKey(), "key" + i++);
        }
        assertEquals(i, 9);
    }

    @Test
    public void testEmpty() {
        String bucket = "test-empty";

        server.putBucket(bucket);

        BucketObjectIterator iterator =
                new BucketObjectIterator(client, bucket, Collections.singletonList(""));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testInvalidListAll() {
        String bucket = "test-invalid-list-all";

        List<String> subObjects = new ArrayList<>();
        subObjects.add("");
        subObjects.add(bucket);

        BucketObjectIterator iterator =
                new BucketObjectIterator(client, bucket, subObjects);

        boolean ex = false;
        try {
            iterator.hasNext();
        } catch (IllegalArgumentException e) {
            ex = e.getMessage().startsWith("multiple");
        }
        assertTrue(ex);
    }
}


