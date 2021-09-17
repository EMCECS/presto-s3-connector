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
package com.facebook.presto.s3.reader;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.s3.S3AccessObject;
import com.facebook.presto.s3.S3ColumnHandle;
import com.facebook.presto.s3.S3TableLayoutHandle;
import com.facebook.presto.s3.decoder.MetadataFieldValueProvider;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MetadataRecordReader
        implements RecordReader
{
    private final List<S3ColumnHandle> columnHandles;

    private final Iterator<Map<String, Object>> lines;

    public MetadataRecordReader(List<S3ColumnHandle> columnHandles, S3AccessObject accessObject, S3TableLayoutHandle table)
    {
        this.columnHandles = columnHandles;
        this.lines = accessObject.listObjectMetadata(table.getTable().getTableName()).iterator();
    }

    @Override
    public long getTotalBytes()
    {
        return 0;
    }

    @Override
    public boolean hasNext()
    {
        return lines.hasNext();
    }

    @Override
    public Map<DecoderColumnHandle, FieldValueProvider> next()
    {
        Map<String, Object> values = lines.next();

        Map<DecoderColumnHandle, FieldValueProvider> valueProviderMap = new HashMap<>();

        for (int i = 0; i < columnHandles.size(); i++) {
            valueProviderMap.put(columnHandles.get(i),
                    new MetadataFieldValueProvider(columnHandles.get(i), values));
        }

        return valueProviderMap;
    }

    @Override
    public void close()
    {
    }
}
