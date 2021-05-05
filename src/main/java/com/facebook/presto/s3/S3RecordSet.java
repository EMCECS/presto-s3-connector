/*
 * Copyright (c) Pravega Authors.
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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.s3.S3Const.CSV;
import static com.facebook.presto.s3.S3Const.SESSION_PROP_NEW_RECORD_CURSOR;
import static com.facebook.presto.s3.S3Util.boolProp;
import static com.facebook.presto.s3.S3Util.constructReaderProps;
import static java.util.Objects.requireNonNull;

public class S3RecordSet
        implements RecordSet {
    private final ConnectorSession session;
    private final S3Split split;
    private final List<S3ColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final String tablename;
    private final String schemaname;
    private final S3AccessObject accessObject;
    private final RowDecoder objectDecoder;
    private final S3ObjectRange objectRange;
    private final S3TableHandle s3TableHandle;

    public S3RecordSet(ConnectorSession session,
                       S3Split split,
                       List<S3ColumnHandle> columnHandles,
                       S3AccessObject accessObject,
                       RowDecoder objectDecoder,
                       S3TableHandle s3TableHandle)

    {
        this.session = requireNonNull(session, "session is null");
        this.split = requireNonNull(split, "split is null");
        requireNonNull(accessObject, "s3Helper is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.accessObject = requireNonNull(accessObject, "accessObject is null");
        this.objectDecoder = requireNonNull(objectDecoder, "rowDecoder is null");
        this.s3TableHandle = requireNonNull(s3TableHandle, "s3TableHandle is null");
        this.objectRange =
                S3ObjectRange.deserialize(
                        requireNonNull(
                                split.getObjectRange(), "this.objectRange is null because split.getObjectRange returned null"));
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (S3ColumnHandle column : columnHandles) {
            types.add(column.getType());
        }

        this.columnTypes = types.build();
        this.tablename = split.getS3TableHandle().getTableName();
        this.schemaname = split.getS3TableHandle().getSchemaName();
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        try {
                boolean isCSV = s3TableHandle.getObjectDataFormat().equals(CSV);
                if (boolProp(session, SESSION_PROP_NEW_RECORD_CURSOR, false) && isCSV) {
                return new S3RecordCursor2(columnHandles,
                        split.getS3TableLayoutHandle(),
                        accessObject,
                        objectRange,
                        constructReaderProps(session));
            }
            else {
                return new S3RecordCursor(columnHandles,
                        split.getS3TableLayoutHandle(),
                        accessObject,
                        objectDecoder,
                        objectRange,
                        split.getS3SelectPushdownEnabled(),
                        s3TableHandle);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
