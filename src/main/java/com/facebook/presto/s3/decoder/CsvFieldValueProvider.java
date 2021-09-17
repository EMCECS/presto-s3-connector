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
package com.facebook.presto.s3.decoder;

import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.s3.S3RecordImpl;
import io.airlift.slice.Slice;

public class CsvFieldValueProvider
        extends FieldValueProvider
{
    public final int field;

    private final S3RecordImpl record;

    public CsvFieldValueProvider(S3RecordImpl record, int field)
    {
        this.record = record;
        this.field = field;
    }

    @Override
    public boolean getBoolean()
    {
        return record.getBoolean(field);
    }

    @Override
    public long getLong()
    {
        return record.getLong(field);
    }

    @Override
    public double getDouble()
    {
        return record.getDouble(field);
    }

    @Override
    public Slice getSlice()
    {
        return record.getSlice(field);
    }

    @Override
    public boolean isNull()
    {
        return record.isNull(field);
    }
}
