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
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.*;

public class S3ObjectRangeIterator
        implements Iterator<S3ObjectRange>
{
    private final AmazonS3 s3Client;
    private final int rangeBytes;
    private final int batchSize = 100;

    private final Iterator<Map.Entry<String, List<String>>> source;
    private BucketObjectIterator bucketObject;
    private Iterator<S3ObjectRange> range;

    private Pair<S3ObjectSummary, Long> continuation;

    public S3ObjectRangeIterator(AmazonS3 s3Client, Map<String, List<String>> sourceMap, int rangeMB) {
        this.s3Client = s3Client;
        this.rangeBytes = rangeMB * 1024 * 1024;
        this.source = sourceMap.entrySet().iterator();
    }

    private boolean advance() {
        if (bucketObject == null || !bucketObject.hasNext()) {
            if (!source.hasNext()) {
                return false;
            }

            Map.Entry<String, List<String>> entry = source.next();
            bucketObject = new BucketObjectIterator(s3Client,
                    entry.getKey(),
                    entry.getValue() != null
                            ? entry.getValue()
                            : Collections.singletonList(""));
        }

        List<S3ObjectRange> rangeList = new ArrayList<>();

        for (int count = 0; count < batchSize &&
                continuation != null || bucketObject.hasNext();) {
            S3ObjectSummary object = continuation == null
                    ? bucketObject.next()
                    : continuation.getLeft();

            if (object.getSize() == 0) {
                continue;
            }

            if (rangeBytes == 0) {
                count++;
                rangeList.add(new S3ObjectRange(object.getBucketName(), object.getKey()));
            } else {
                long offset = continuation == null
                        ? 0
                        : continuation.getRight();
                while (offset == 0 || offset < object.getSize()) {
                    int length = (int) (offset + rangeBytes > object.getSize()
                            ? object.getSize() - offset
                            : rangeBytes);
                    rangeList.add(new S3ObjectRange(object.getBucketName(), object.getKey(), offset, length));
                    offset += rangeBytes;

                    if (count++ >= batchSize) {
                        continuation = offset < object.getSize()
                                ? new Pair<>(object, offset)
                                : null;
                        break;
                    }
                }
            }
        }

        range = rangeList.iterator();
        return range.hasNext();
    }

    @Override
    public boolean hasNext() {
        return (range != null && range.hasNext()) || advance();
    }

    @Override
    public S3ObjectRange next() {
        return range.next();
    }
}