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
import com.facebook.presto.s3.util.SimpleS3Server;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.testng.Assert.*;

public class S3ObjectRangeIteratorTest {

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
    public void testEmptyBucket() {
        server.putKey("bucket1", "key1-1", new byte[]{'a'});
        server.putKey("bucket1", "key1-2", new byte[]{'a'});
        server.putKey("bucket1", "key1-3", new byte[]{'a'});

        server.putBucket("bucket2");

        server.putKey("bucket3", "key3-1", new byte[]{'a'});
        server.putKey("bucket3", "key3-2", new byte[]{'a'});

        Map<String, List<String>> sources = new TreeMap<>();
        sources.put("bucket1", null);
        sources.put("bucket2", null);
        sources.put("bucket3", null);

        S3ObjectRangeIterator iterator =
                new S3ObjectRangeIterator(client, sources, 100);

        List<S3ObjectRange> ranges = new ArrayList<>();
        while (iterator.hasNext()) {
            ranges.add(iterator.next());
        }

        assertEquals(ranges.size(), 5);

        int i = 0;
        assertEquals(ranges.get(i++).getKey(), "key1-1");
        assertEquals(ranges.get(i++).getKey(), "key1-2");
        assertEquals(ranges.get(i++).getKey(), "key1-3");
        assertEquals(ranges.get(i++).getKey(), "key3-1");
        assertEquals(ranges.get(i).getKey(), "key3-2");
    }

    @Test
    public void testSplitObject() {
        String bucket = "test-split-object";
        server.putKey(bucket, "key1", new byte[4096]);

        Map<String, List<String>> sources = new TreeMap<>();
        sources.put(bucket, null);

        S3ObjectRangeIterator iterator =
                new S3ObjectRangeIterator(client, sources, 1024);

        S3ObjectRange range;

        assertTrue(iterator.hasNext());
        range = iterator.next();
        assertEquals(range.getBucket(), bucket);
        assertEquals(range.getKey(), "key1");
        assertEquals(range.getOffset(), 0);
        assertEquals(range.getLength(), 1024);

        assertTrue(iterator.hasNext());
        range = iterator.next();
        assertEquals(range.getBucket(), bucket);
        assertEquals(range.getKey(), "key1");
        assertEquals(range.getOffset(), 1024);
        assertEquals(range.getLength(), 1024);

        assertTrue(iterator.hasNext());
        range = iterator.next();
        assertEquals(range.getBucket(), bucket);
        assertEquals(range.getKey(), "key1");
        assertEquals(range.getOffset(), 2048);
        assertEquals(range.getLength(), 1024);

        assertTrue(iterator.hasNext());
        range = iterator.next();
        assertEquals(range.getBucket(), bucket);
        assertEquals(range.getKey(), "key1");
        assertEquals(range.getOffset(), 3072);
        assertEquals(range.getLength(), 1024);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testSplitResume() {
        String bucket = "test-split-resume";
        server.putKey(bucket, "key1", new byte[2048]);

        Map<String, List<String>> sources = new TreeMap<>();
        sources.put(bucket, null);

        S3ObjectRangeIterator iterator =
                new S3ObjectRangeIterator(client, sources, 1024, 1);

        S3ObjectRange range;

        assertTrue(iterator.hasNext());
        range = iterator.next();
        assertEquals(range.getOffset(), 0);
        assertEquals(range.getLength(), 1024);

        assertTrue(iterator.hasNext());
        range = iterator.next();
        assertEquals(range.getOffset(), 1024);
        assertEquals(range.getLength(), 1024);

        assertFalse(iterator.hasNext());
    }
}
