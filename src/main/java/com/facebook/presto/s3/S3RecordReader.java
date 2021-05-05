/*
 * Copyright (c) Pravega Authors.
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static com.google.common.base.Preconditions.checkState;

public class S3RecordReader {
    private S3AccessObject s3;
    private S3Table table;
    private Map<String, List<String>> bucketObjectMap;
    private InputStream inputStream;

    private Iterator<String> bucketIterator;
    private Iterator<String> keyIterator;
    private String curBucket;
    private String curKey = null;
    private int streamPosition = -1;
    private int streamLength = -1;
    private int bytesRemaining = -1;

    private int MAX_BYTES_IN_ROW = 8388608; // 8MiB

    public S3RecordReader(S3AccessObject s3, S3Table table, Map<String, List<String>> bucketObjectMap) {
        requireNonNull(s3, "s3 is null");
        requireNonNull(table, "table is null");
        requireNonNull(bucketObjectMap, "bucketObjectMap is null");
        checkState(!bucketObjectMap.keySet().isEmpty(), "bucketObjectMap has no keys");

        this.s3 =  s3;
        this.table = table;
        this.bucketObjectMap = bucketObjectMap;
        this.bucketIterator = bucketObjectMap.keySet().iterator();

        do {
            this.curBucket = bucketIterator.next();
            this.keyIterator = bucketObjectMap.get(this.curBucket).iterator();

            if(this.keyIterator.hasNext()) {
                this.curKey = this.keyIterator.next();
            }
        } while (this.curKey == null && this.bucketIterator.hasNext());

        checkState(this.curKey != null, "all values of bucketObjectMap are empty lists");

        this.inputStream = openStream(this.curBucket, this.curKey);
    }

    public String getNextLine() {
        String val = "";
        do {
            if (this.bytesRemaining <= 0) {
                try {
                    this.inputStream.close();
                } catch (IOException ignored) {

                }
                this.inputStream = null;
                while (this.inputStream == null && advanceIterators() == 1) {
                    this.inputStream = openStream(curBucket, curKey);
                }

                if (this.inputStream == null) {
                    return null;
                }
            }

            val = getNextLineCSV();
        } while(val == null);
        return val;
    }

    private String getNextLineCSV() {
        byte[] line = new byte[MAX_BYTES_IN_ROW+1];
        int n = 0, i = 0;
        String lastChar = null;
        for(; i < MAX_BYTES_IN_ROW && bytesRemaining > 0; i+=n, streamPosition++, bytesRemaining--) {
            try {
                n = inputStream.read(line, i, 2);
                byte[] lastCharArr = {line[i], line[i+1]};
                lastChar = new String(lastCharArr, StandardCharsets.UTF_16);
                if(n < 2 || lastChar.equals("\n")) {
                    break;
                }
            } catch(IOException e) {
                this.bytesRemaining = 0;
                return null;
            }
        }
        if(lastChar.equals("\n")) {
            line[i] = 0;
            line[i + 1] = 0;
        }

        return new String(line, StandardCharsets.UTF_16);
    }

    private InputStream openStream(String bucket, String key) {
        try {
            InputStream stream = s3.getObject(bucket, key);
            streamPosition = 0;
            streamLength = stream.available();
            bytesRemaining = streamLength;
            return stream;
        } catch (Exception e) {
            return null;
        }
    }

    private int advanceIterators() {
        if(keyIterator.hasNext()) {
            curKey = keyIterator.next();
            return 1;
        }

        while (bucketIterator.hasNext()) {
            curBucket = bucketIterator.next();
            keyIterator = bucketObjectMap.get(curBucket).iterator();
            if(keyIterator.hasNext()) {
                curKey = keyIterator.next();
                return 1;
            }
        }
        return 0;
    }
}
