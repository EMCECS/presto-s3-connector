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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.decoder.DecoderErrorCode;
import com.facebook.presto.s3.reader.CsvRecordReader;
import com.facebook.presto.s3.reader.RecordReader;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;

import static com.facebook.presto.common.type.Varchars.truncateToLength;
import static com.google.common.base.Preconditions.checkArgument;

public class S3RecordCursor2
        implements RecordCursor
{
    private static final Logger log = Logger.get(S3RecordCursor2.class);

    private S3Record record;

    private final RecordReader recordReader;

    private final List<S3ColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    public S3RecordCursor2(
            List<S3ColumnHandle> columnHandles,
            S3TableLayoutHandle s3TableLayoutHandle,
            S3AccessObject accessObject,
            S3ObjectRange objectRange,
            S3ReaderProps readerProps)
    {
        this.columnHandles = columnHandles;

        this.recordReader = new CsvRecordReader(columnHandles,
                accessObject,
                objectRange,
                s3TableLayoutHandle,
                readerProps);

        this.fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            this.fieldToColumnIndex[i] = columnHandles.get(i).getOrdinalPosition();
        }
    }

    @Override
    public long getCompletedBytes()
    {
        // TODO: https://github.com/EMCECS/presto-s3-connector/issues/28
        return lineReader.bytesProcessed();
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        record = recordReader.advance();
        return record != null;
    }

    @Override
    public boolean getBoolean(int field)
    {
        if (!record.decoded) {
            record.decode(); // move to s3record impl
        }
        return record.getBoolean(fieldToColumnIndex[field]);
    }

    @Override
    public long getLong(int field)
    {
        if (!record.decoded) {
            record.decode();
        }
        return record.getLong(fieldToColumnIndex[field]);
    }

    @Override
    public double getDouble(int field)
    {
        if (!record.decoded) {
            record.decode();
        }
        return record.getDouble(fieldToColumnIndex[field]);
    }

    @Override
    public Slice getSlice(int field)
    {
        if (!record.decoded) {
            record.decode();
        }
        return Slices.copyOf(truncateToLength(record.getSlice(fieldToColumnIndex[field]), VarcharType.VARCHAR));
    }

    @Override
    public Object getObject(int field)
    {
        throw new PrestoException(DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED, "conversion to block not supported");
    }

    @Override
    public boolean isNull(int field)
    {
        if (!record.decoded) {
            record.decode();
        }
        return record.isNull(fieldToColumnIndex[field]);
    }

    @Override
    public void close()
    {
        recordReader.close();
    }
}
