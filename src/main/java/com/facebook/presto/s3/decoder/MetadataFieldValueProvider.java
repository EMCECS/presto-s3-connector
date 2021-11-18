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
import com.facebook.presto.s3.S3ColumnHandle;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Map;

public class MetadataFieldValueProvider
        extends FieldValueProvider {
    private final S3ColumnHandle column;

    private final Map<String, Object> valueMap;

    public MetadataFieldValueProvider(S3ColumnHandle column, Map<String, Object> valueMap) {
        this.column = column;
        this.valueMap = valueMap;
    }

    @Override
    public boolean getBoolean() {
        return Boolean.parseBoolean(String.valueOf(value()));
    }

    @Override
    public long getLong() {
        return Long.parseLong(String.valueOf(value()));
    }

    @Override
    public double getDouble() {
        return Double.parseDouble(String.valueOf(value()));
    }

    @Override
    public Slice getSlice() {
        return Slices.utf8Slice(String.valueOf(value()));
    }

    @Override
    public boolean isNull() {
        return value() == null;
    }

    private Object value() {
        return valueMap.get(column.getName().toLowerCase());
    }
}
