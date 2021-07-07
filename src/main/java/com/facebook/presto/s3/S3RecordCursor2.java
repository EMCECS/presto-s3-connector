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
import com.facebook.presto.s3.IonSqlQueryBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.InputStream;
import java.util.List;

import static com.facebook.presto.common.type.Varchars.truncateToLength;
import static com.facebook.presto.s3.S3Const.CSV;
import static com.facebook.presto.s3.S3Const.LC_TRUE;
import static com.facebook.presto.s3.S3Const.TEXT;
import static com.google.common.base.Preconditions.checkArgument;

public class S3RecordCursor2
        implements RecordCursor
{
    private static final Logger log = Logger.get(com.facebook.presto.s3.S3RecordCursor2.class);

    private final S3AccessObject accessObject;
    private final S3ObjectRange objectRange;
    private final S3ReaderProps readerProps;
    private final S3TableLayoutHandle s3TableLayoutHandle;

    private BytesLineReader lineReader = null;
    private final S3Record record;

    private final List<S3ColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    public S3RecordCursor2(
            List<S3ColumnHandle> columnHandles,
            S3TableLayoutHandle s3TableLayoutHandle,
            S3AccessObject accessObject,
            S3ObjectRange objectRange,
            S3ReaderProps readerProps)
    {

        if (readerProps.getS3SelectEnabled()) {
            // setting s3SelectPushdownEnabled currently checks format = csv - sanity check that here
            if (!s3TableLayoutHandle.getTable().getObjectDataFormat().equals(CSV) &&
                    !s3TableLayoutHandle.getTable().getObjectDataFormat().equals(TEXT)) {
                throw new IllegalArgumentException("s3SelectPushdownEnabled for non delim file");
            }
        }

        this.columnHandles = columnHandles;
        this.accessObject = accessObject;
        this.objectRange = objectRange;
        this.readerProps = readerProps;
        this.s3TableLayoutHandle= s3TableLayoutHandle;

        String fieldDelim = s3TableLayoutHandle.getTable().getFieldDelimiter();
        if (fieldDelim.length() != 1) {
            throw new IllegalArgumentException(fieldDelim);
        }
        this.record = new S3Record(fieldDelim.charAt(0));

        this.fieldToColumnIndex = new int[columnHandles.size()];

        for (int i = 0; i < columnHandles.size(); i++) {
            S3ColumnHandle columnHandle = columnHandles.get(i);
            this.fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }
    }

    private void init()
    {
        long end = objectRange.getLength() == -1L
                ? Long.MAX_VALUE
                : objectRange.getOffset() + objectRange.getLength();

        long start = objectRange.getOffset();
        if (readerProps.getS3SelectEnabled() && objectRange.getLength() > 0) {
            // if using scan range with s3 select there is no seeking/spillover here
            start = 0;
            end = Long.MAX_VALUE;
        }

        lineReader = new BytesLineReader(
                objectStream(),
                readerProps.getBufferSizeBytes(),
                start, end);

        if(!readerProps.getS3SelectEnabled() &&
                objectRange.getOffset() == 0 &&
                s3TableLayoutHandle.getTable().getHasHeaderRow().equals(LC_TRUE)) {
            // eat the header
            lineReader.read(record.value);
        }
    }

    private InputStream objectStream()
    {
        if (readerProps.getS3SelectEnabled()) {
            String sql = new IonSqlQueryBuilder()
                    .buildSql(S3RecordCursor::s3SelectColumnMapper,
                            S3RecordCursor::s3SelectTypeMapper,
                            columnHandles,
                            s3TableLayoutHandle.getConstraints());
            log.info("s3select " + objectRange + ", " + sql);
            final boolean hasHeaderRow = s3TableLayoutHandle.getTable().getHasHeaderRow().equals(LC_TRUE);
            final String recordDelimiter = s3TableLayoutHandle.getTable().getRecordDelimiter();
            final String fieldDelimiter = s3TableLayoutHandle.getTable().getFieldDelimiter();

            return accessObject.selectObjectContent(objectRange, sql,
                    new S3SelectProps(hasHeaderRow, recordDelimiter, fieldDelimiter));
        }
        else {
            return accessObject.getObject(objectRange.getBucket(), objectRange.getKey(), objectRange.getOffset());
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
        if (lineReader == null) {
            init();
        }

        record.len = lineReader.read(record.value);
        record.decoded = false;
        return record.len >= 0;
    }

    @Override
    public boolean getBoolean(int field)
    {
        if (!record.decoded) {
            record.decode();
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
        if (lineReader != null) {
            lineReader.close();
        }
    }
}
