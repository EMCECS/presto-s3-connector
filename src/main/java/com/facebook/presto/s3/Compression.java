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
import io.airlift.compress.snappy.SnappyCodec;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.util.HashMap;
import java.util.Map;

public class Compression {
    private static Map<String, CompressionCodec> codecMap = new HashMap<>();

    static {
        CompressionCodec codec;

        codec = new JdkGzipCodec();
        codecMap.put(codec.getDefaultExtension(), codec);

        codec = new SnappyCodec();
        codecMap.put(codec.getDefaultExtension(), codec);
    }

    public static CompressionCodec getCodec(String key) {
        int idx = key.lastIndexOf(".");
        return idx < 0 ? null : codecMap.get(key.substring(idx));
    }
}
