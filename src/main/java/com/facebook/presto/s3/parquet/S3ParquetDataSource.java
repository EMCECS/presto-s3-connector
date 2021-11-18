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

package com.facebook.presto.s3.parquet;

import com.facebook.presto.parquet.AbstractParquetDataSource;
import com.facebook.presto.parquet.ParquetDataSourceId;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

import static com.facebook.presto.s3.S3ErrorCode.S3_FILESYSTEM_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class S3ParquetDataSource extends AbstractParquetDataSource {
    private final FSDataInputStream inputStream;

    public S3ParquetDataSource(ParquetDataSourceId id, FSDataInputStream inputStream) {
        super(id);
        this.inputStream = requireNonNull(inputStream, "inputStream is null");
    }

    @Override
    public void close()
            throws IOException {
        inputStream.close();
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength) {
        try {
            inputStream.readFully(position, buffer, bufferOffset, bufferLength);
        } catch (PrestoException e) {
            // just in case there is a Presto wrapper or hook
            throw e;
        } catch (Exception e) {
            throw new PrestoException(S3_FILESYSTEM_ERROR, format("Error reading from %s at position %s", getId(), position), e);
        }
    }

    public static S3ParquetDataSource buildS3ParquetDataSource(FSDataInputStream inputStream, String bucketObjectName) {
        return new S3ParquetDataSource(new ParquetDataSourceId(bucketObjectName), inputStream);
    }
}
