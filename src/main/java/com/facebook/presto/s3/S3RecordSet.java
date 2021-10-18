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
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.s3.reader.AvroRecordReader;
import com.facebook.presto.s3.reader.CsvRecordReader;
import com.facebook.presto.s3.reader.JsonRecordReader;
import com.facebook.presto.s3.reader.RecordReader;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Supplier;

import static com.facebook.presto.s3.S3Const.*;
import static com.facebook.presto.s3.S3Util.constructReaderProps;
import static com.facebook.presto.s3.S3Util.delimitedFormat;
import static java.util.Objects.requireNonNull;

public class S3RecordSet
        implements RecordSet {
    private static final Logger log = Logger.get(S3RecordSet.class);

    private final ConnectorSession session;
    private final S3Split split;
    private final List<S3ColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final S3AccessObject accessObject;
    private final RowDecoder objectDecoder;
    private final S3ObjectRange objectRange;
    private final S3TableHandle s3TableHandle;

    public S3RecordSet(ConnectorSession session,
                       S3Split split,
                       List<S3ColumnHandle> columnHandles,
                       S3AccessObject accessObject,
                       RowDecoder objectDecoder,
                       S3TableHandle s3TableHandle) {
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
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        RecordReader recordReader;
        final S3ReaderProps readerProps = constructReaderProps(session);
        Supplier<CountingInputStream> inputStreamSupplier = () -> objectStream(readerProps);

        switch (s3TableHandle.getObjectDataFormat()) {
            case CSV:
            case TEXT:
                recordReader = new CsvRecordReader(columnHandles,
                        objectRange,
                        split.getS3TableLayoutHandle(),
                        readerProps,
                        inputStreamSupplier);
                break;
            case JSON:
                recordReader = new JsonRecordReader(objectDecoder, objectRange, readerProps, inputStreamSupplier);
                break;
            case AVRO:
                recordReader = new AvroRecordReader(columnHandles, objectRange, inputStreamSupplier);
                break;
            default:
                throw new IllegalArgumentException("unhandled type " + s3TableHandle.getObjectDataFormat());
        }

        return new S3RecordCursor(recordReader, columnHandles);
    }

    private CountingInputStream objectStream(S3ReaderProps readerProps)
    {
        if (readerProps.getS3SelectEnabled() && delimitedFormat(s3TableHandle.getObjectDataFormat())) {
            String sql = new IonSqlQueryBuilder()
                    .buildSql(S3RecordSet::s3SelectColumnMapper,
                            S3RecordSet::s3SelectTypeMapper,
                            columnHandles,
                            split.getS3TableLayoutHandle().getConstraints());
            log.info("s3select " + objectRange + ", " + sql);
            final boolean hasHeaderRow = s3TableHandle.getHasHeaderRow().equals(LC_TRUE);
            final String recordDelimiter = s3TableHandle.getRecordDelimiter();
            final String fieldDelimiter = s3TableHandle.getFieldDelimiter();

            return new CountingInputStream(accessObject.selectObjectContent(objectRange, sql,
                    new S3SelectProps(hasHeaderRow, recordDelimiter, fieldDelimiter)));
        }
        else {
            FSDataInputStream stream =
                    accessObject.getFsDataInputStream(objectRange.getBucket(), objectRange.getKey(), readerProps.getBufferSizeBytes());
            try {
                stream.seek(objectRange.getOffset()); // changes position, no call yet
            }
            catch (IOException  e) {
                throw new UncheckedIOException(e);
            }
            return new CountingInputStream(stream);
        }
    }

    static Integer s3SelectColumnMapper(ColumnHandle columnHandle)
    {
        return ((S3ColumnHandle) columnHandle).getAbsoluteSchemaPosition();
    }

    static Type s3SelectTypeMapper(ColumnHandle columnHandle)
    {
        return ((S3ColumnHandle) columnHandle).getType();
    }
}
