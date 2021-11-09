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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;

import static com.google.common.base.MoreObjects.toStringHelper;

public class S3ObjectRange
        implements Serializable
{
    private final String bucket;
    private final String key;
    private final long offset;
    private final int length;
    private final boolean split;
    private final String compressionType;

    public S3ObjectRange(String bucket, String key, long offset, int length, boolean split)
    {
        this(bucket, key, offset, length, split, null);
    }

    public S3ObjectRange(String bucket, String key, long offset, int length, boolean split, String compressionType)
    {
        this.bucket = bucket;
        this.key = key;
        this.offset = offset;
        this.length = length;
        this.split = split;
        this.compressionType = compressionType;
    }

    public String getBucket()
    {
        return bucket;
    }

    public String getKey()
    {
        return key;
    }

    public long getOffset()
    {
        return offset;
    }

    public int getLength()
    {
        return length;
    }

    public boolean getSplit()
    {
        return split;
    }

    public String getCompressionType()
    {
        return compressionType;
    }

    public static byte[] serialize(com.facebook.presto.s3.S3ObjectRange objectRange)
    {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(objectRange);
            return baos.toByteArray();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static com.facebook.presto.s3.S3ObjectRange deserialize(byte[] objectRangeBytes)
    {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(objectRangeBytes);
            ObjectInputStream bis = new ObjectInputStream(bais);
            return (com.facebook.presto.s3.S3ObjectRange) bis.readObject();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bucket", bucket)
                .add("key", key)
                .add("offset", offset)
                .add("length", length)
                .add("compressionType", compressionType)
                .toString();
    }
}
