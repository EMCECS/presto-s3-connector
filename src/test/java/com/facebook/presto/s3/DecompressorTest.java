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

import org.apache.hadoop.io.compress.CompressionCodec;
import org.testng.annotations.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class DecompressorTest {
    Map<String, InputStream> compressedData = new HashMap<>();

    @Test
    public void testGzip() {
        addCompressedData("key1.gz", "value-for-key1");
        assertEquals(readToString(getInputStream("key1.gz")), "value-for-key1");
    }

    @Test
    public void testSnappy() {
        addCompressedData("key2.snappy", "value-for-key2");
        assertEquals(readToString(getInputStream("key2.snappy")), "value-for-key2");
    }

    InputStream getInputStream(String f) {
        try {
            CompressionCodec codec = Compression.getCodec(f);
            assertNotNull(codec);
            return codec.createInputStream(compressedData.containsKey(f)
                    ? compressedData.get(f)
                    : DecompressorTest.class.getResourceAsStream(f));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void addCompressedData(String f, String data) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStream os = Compression.getCodec(f).createOutputStream(baos);

            os.write(data.getBytes(StandardCharsets.UTF_8));
            os.close();

            compressedData.put(f, new ByteArrayInputStream(baos.toByteArray()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    String readToString(InputStream inputStream) {
        try (Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
            StringBuilder sb = new StringBuilder();

            int c;
            do {
                c = reader.read();
                if (c == -1) {
                    break;
                }
                sb.append((char) c);
            } while (true);
            return sb.toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
