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
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.util.*;

public class S3ObjectRangeIterator
        implements Iterator<S3ObjectRange>
{
    private final AmazonS3 s3Client;
    private final long rangeBytes;
    private final int batchSize;

    private final Iterator<Map.Entry<String, List<String>>> source;
    private BucketObjectIterator bucketObject;
    private Iterator<S3ObjectRange> range;

    private Pair<S3ObjectSummary, Long> continuation;

    public S3ObjectRangeIterator(AmazonS3 s3Client, Map<String, List<String>> sourceMap, long rangeBytes) {
        this(s3Client, sourceMap, rangeBytes, 100);
    }

    public S3ObjectRangeIterator(AmazonS3 s3Client, Map<String, List<String>> sourceMap, long rangeBytes, int batchSize) {
        this.s3Client = s3Client;
        this.rangeBytes = rangeBytes;
        this.source = sourceMap.entrySet().iterator();
        this.batchSize = batchSize;
    }

    private boolean advance() {
        do {
            if (continuation == null &&
                    (bucketObject == null || !bucketObject.hasNext())) {
                if (!source.hasNext()) {
                    return false;
                }

                Map.Entry<String, List<String>> entry = source.next();

                checkBucketExists(entry.getKey());

                bucketObject = new BucketObjectIterator(s3Client,
                        entry.getKey(),
                        entry.getValue() != null
                                ? entry.getValue()
                                : Collections.singletonList(""));
            }

            if (nextRangeList()) {
                return true;
            }
        } while (true);
    }

    private void checkBucketExists(String bucket) {
        // normalize returned errors from different s3 apis
        boolean exists = false;
        try {
            exists = s3Client.doesBucketExistV2(bucket);
        } catch (AmazonS3Exception e) {
            if (!e.getErrorCode().equalsIgnoreCase("InvalidBucketName")) {
                throw e;
            }
        }

        if (!exists) {
            throw new IllegalArgumentException("Bucket '" + bucket + "' does not exist");
        }
    }

    private boolean nextRangeList() {
        List<S3ObjectRange> rangeList = new ArrayList<>();

        for (int count = 0; count < batchSize &&
                continuation != null || bucketObject.hasNext();) {

            S3ObjectSummary object = continuation == null
                    ? bucketObject.next()
                    : continuation.getLeft();
            long offset = continuation == null
                    ? 0
                    : continuation.getRight();
            continuation = null;

            if (object.getSize() == 0) {
                continue;
            }

            // offset 0 is start of new object, check if compressed
            // in this case do not split
            CompressionCodec codec = offset == 0
                    ? Compression.getCodec(object.getKey())
                    : null;

            if (codec != null || rangeBytes == 0) {
                rangeList.add(new S3ObjectRange(object.getBucketName(),
                        object.getKey(),
                        0, (int) object.getSize(),
                        false,
                        codec == null ? null : codec.getDefaultExtension()));
                if (++count >= batchSize) {
                    break;
                }
            } else {
                while (offset == 0 || offset < object.getSize()) {
                    int length = (int) (offset + rangeBytes > object.getSize()
                            ? object.getSize() - offset
                            : rangeBytes);
                    rangeList.add(new S3ObjectRange(object.getBucketName(), object.getKey(), offset, length, true));
                    offset += rangeBytes;

                    if (++count >= batchSize) {
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
