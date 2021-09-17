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
import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;

import com.facebook.presto.s3.reader.RecordReader;
import com.facebook.presto.spi.RecordCursor;

import io.airlift.slice.Slice;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

public class S3RecordCursor
        implements RecordCursor {
    private static final Logger log = Logger.get(S3RecordCursor.class);

    private final List<S3ColumnHandle> columnHandles;
    private final FieldValueProvider[] currentRowValues;

    private final RecordReader recordReader;

    public S3RecordCursor(RecordReader recordReader,
                          List<S3ColumnHandle> columnHandles)
    {
        this.recordReader = recordReader;
        this.columnHandles = columnHandles;
        this.currentRowValues = new FieldValueProvider[columnHandles.size()];
    }

    @Override
    public long getCompletedBytes()
    {
        return 0; // TODO: need this from each reader
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    @Override
    public boolean advanceNextPosition() {
        if (recordReader.hasNext()) {
            return false;
        }

        Map<DecoderColumnHandle, FieldValueProvider> values = recordReader.next();
        for (int i = 0; i < columnHandles.size(); i++) {
            currentRowValues[i] = values.get(columnHandles.get(i));
        }

        return true;
    }

    @Override
    public boolean getBoolean(int field) {
        return currentRowValues[field].getBoolean();
    }

    @Override
    public long getLong(int field) {
        return currentRowValues[field].getLong();
    }

    @Override
    public double getDouble(int field) {
        return currentRowValues[field].getDouble();
    }

    @Override
    public Slice getSlice(int field) {
        return currentRowValues[field].getSlice();
    }

    @Override
    public Object getObject(int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return currentRowValues[field] == null || currentRowValues[field].isNull();
    }

    @Override
    public void close()
    {
    }
}
