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

import io.airlift.compress.gzip.JdkGzipCodec;
import io.airlift.compress.lz4.Lz4Codec;
import io.airlift.compress.snappy.SnappyCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.testng.annotations.Test;

import java.io.*;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.*;
import static org.testng.Assert.assertTrue;

public class DecompressorTest {
    private final Map<String, byte[]> compressedData = new HashMap<>();

    private final SecureRandom random = new SecureRandom();

    @Test
    public void testGzip() {
        test("keyA.gz", "gz");
    }

    @Test
    public void testSnappy() {
        test("keyB.snappy", "snappy");
    }

    @Test
    public void testLz4() {
        test("keyC.lz4", "lz4");
    }

    @Test
    public void testNotSupported() {
        assertNull(Compression.getCodec("key.bz2"));
    }

    @Test
    public void testNoExtension() {
        assertNull(Compression.getCodec("key"));
    }

    @Test
    public void testCodecFromType() {
        assertNull(Compression.getCodecFromType(null));
        assertNull(Compression.getCodecFromType(".xyz"));
        assertTrue(Compression.getCodecFromType(".gz") instanceof JdkGzipCodec);
        assertTrue(Compression.getCodecFromType(".snappy") instanceof SnappyCodec);
        assertTrue(Compression.getCodecFromType(".lz4") instanceof Lz4Codec);
    }

    private void test(String key, String format) {
        byte[] data = new byte[1024];
        random.nextBytes(data);

        CompressionCodec codec = addCompressedData(key, data);
        switch (format) {
            case "gz":
                assertTrue(codec instanceof JdkGzipCodec);
                break;
            case "snappy":
                assertTrue(codec instanceof SnappyCodec);
                break;
            case "lz4":
                assertTrue(codec instanceof Lz4Codec);
                break;
            default:
                throw new IllegalArgumentException(format);
        }

        // sanity check data was compressed
        assertNotEquals(data, read(new ByteArrayInputStream(compressedData.get(key))));
        // run through codec, get original data
        assertEquals(data, read(getInflatingStream(key)));
    }

    InputStream getInflatingStream(String f) {
        try {
            CompressionCodec codec = Compression.getCodec(f);
            assertNotNull(codec);
            return codec.createInputStream(new ByteArrayInputStream(compressedData.get(f)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    CompressionCodec addCompressedData(String f, byte[] data) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            CompressionCodec codec = Compression.getCodec(f);
            OutputStream os = codec.createOutputStream(baos);
            os.write(data);
            os.close();
            compressedData.put(f, baos.toByteArray());
            return codec; // so test case can verify what was used
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    byte[] read(InputStream inputStream) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            int n;
            byte[] buf = new byte[4096];
            do {
                n = inputStream.read(buf);
                if (n > 0) {
                    baos.write(buf, 0, n);
                }
            } while (n > 0);
            return baos.toByteArray();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
