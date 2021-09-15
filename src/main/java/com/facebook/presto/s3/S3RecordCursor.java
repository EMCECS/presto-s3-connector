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
import com.facebook.presto.decoder.RowDecoder;

import com.facebook.presto.decoder.avro.AvroColumnDecoder;
import com.facebook.presto.s3.decoder.MetadataFieldValueProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;

import io.airlift.slice.Slice;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.*;

import static com.facebook.presto.s3.S3Const.*;
import static com.facebook.presto.s3.S3ErrorCode.S3_UNSUPPORTED_FORMAT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Functions.identity;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class S3RecordCursor
        implements RecordCursor {

    protected List<S3ColumnHandle> columnHandles;
    protected final int[] fieldToColumnIndex;
    protected final RowDecoder rowDecoder;
    protected final FieldValueProvider[] currentRowValues;
    protected final S3ObjectRange objectRange;
    protected Iterator<String> lineIterator;
    protected String schemaname;
    protected Iterator<Map<String, Object>> lines;
    protected DatumReader<GenericRecord> reader = new SpecificDatumReader<>();
    protected DataFileStream<GenericRecord> byteReader = null;
    protected String recordDelimiter;
    protected String fieldDelimiter;
    protected Map<DecoderColumnHandle, AvroColumnDecoder> AvroColumnDecoders = null;
    private static final Logger log = Logger.get(com.facebook.presto.s3.S3RecordCursor.class);
    private CountingInputStream objectInputStream;
    private long totalBytes = 0;
    private final S3TableHandle s3TableHandle;

    public S3RecordCursor(
            List<S3ColumnHandle> columnHandles,
            S3TableLayoutHandle s3TableLayoutHandle,
            S3AccessObject accessObject,
            RowDecoder rowDecoder,
            S3ObjectRange objectRange,
            boolean s3SelectPushdownEnabled,
            S3TableHandle s3TableHandle) {
        this.columnHandles = columnHandles;
        this.rowDecoder = rowDecoder;
        this.s3TableHandle = s3TableHandle;
        this.currentRowValues = new FieldValueProvider[columnHandles.size()];
        this.objectRange = requireNonNull(objectRange, "objectRange is null");
        this.schemaname = s3TableLayoutHandle.getTable().getSchemaName();
        this.fieldDelimiter = s3TableHandle.getFieldDelimiter();
        this.recordDelimiter = s3TableHandle.getRecordDelimiter();

        this.schemaname = s3TableLayoutHandle.getTable().getSchemaName();

        if (s3SelectPushdownEnabled) {
            // setting s3SelectPushdownEnabled currently checks format = csv.
            // sanity check that here
            if (!s3TableHandle.getObjectDataFormat().equals(CSV)) {
                throw new IllegalArgumentException("s3SelectPushdownEnabled for non-csv");
            }
        }

        if(this.schemaname.equals("s3_buckets")) {
            this.lines = accessObject.listObjectMetadata(s3TableLayoutHandle.getTable().getTableName()).iterator();
        }
        else if(s3TableHandle.getObjectDataFormat().equals(CSV) ||
                s3TableHandle.getObjectDataFormat().equals(TEXT)) {
            if (s3SelectPushdownEnabled) {
                String sql = new IonSqlQueryBuilder()
                        .buildSql(com.facebook.presto.s3.S3RecordCursor::s3SelectColumnMapper,
                                com.facebook.presto.s3.S3RecordCursor::s3SelectTypeMapper,
                                columnHandles,
                                s3TableLayoutHandle.getConstraints());
                final boolean hasHeaderRow = s3TableHandle.getHasHeaderRow().equals(LC_TRUE);
                final String recordDelimiter = s3TableHandle.getRecordDelimiter();
                final String fieldDelimiter = s3TableHandle.getFieldDelimiter();

                objectInputStream = new CountingInputStream(
                        accessObject.selectObjectContent(objectRange,
                                sql,
                                new S3SelectProps(hasHeaderRow, recordDelimiter, fieldDelimiter)));
            }
            else {
                objectInputStream = new CountingInputStream(accessObject.getObject(objectRange.getBucket(), objectRange.getKey()));
            }

            lineIterator = new CsvLineIterator(objectInputStream);
        }
        else if (s3TableHandle.getObjectDataFormat().equals(AVRO)) {
            try {
                objectInputStream = new CountingInputStream(accessObject.getObject(objectRange.getBucket(), objectRange.getKey()));
                byteReader = new DataFileStream<>(objectInputStream, reader);
            } catch (Exception e) {
                fieldToColumnIndex = null;
                return;
            }
            // Create ColumnDecoders that use column name for mapping, not ordinal position
            List<S3ColumnHandle> avroColumnHandles = new ArrayList<>();
            for (S3ColumnHandle s3ColumnHandle : columnHandles) {
                S3ColumnHandle newHandle = new S3ColumnHandle(s3ColumnHandle.getConnectorId(),
                        s3ColumnHandle.getOrdinalPosition(),
                        s3ColumnHandle.getName(),
                        s3ColumnHandle.getType(),
                        s3ColumnHandle.getName(),  // getMapping returns an ordinal in string format.  We want the name
                        s3ColumnHandle.getDataFormat(),
                        s3ColumnHandle.getFormatHint(),
                        s3ColumnHandle.isKeyDecoder(),
                        s3ColumnHandle.isHidden(),
                        s3ColumnHandle.isInternal());
                avroColumnHandles.add(newHandle);
                }
            this.columnHandles = avroColumnHandles;
            this.AvroColumnDecoders = avroColumnHandles.stream().collect(toImmutableMap(identity(), this::createColumnDecoder));
            log.debug("Avro column handles: " + this.columnHandles.toString());
        } else if(s3TableHandle.getObjectDataFormat().equals(JSON)) {
            objectInputStream = new CountingInputStream(accessObject.getObject(objectRange.getBucket(), objectRange.getKey()));
            ArrayList<String> jsonObject = tempS3ObjectToStringObjectList(objectInputStream);
            lineIterator = jsonObject.iterator();
        } else {
            throw new PrestoException (S3_UNSUPPORTED_FORMAT,
                    format ("Object format type %s is not supported", s3TableHandle.getObjectDataFormat()));
        }

        fieldToColumnIndex = new int[columnHandles.size()];

        if(!s3SelectPushdownEnabled &&
                s3TableHandle.getHasHeaderRow().equals(LC_TRUE) &&
                this.lineIterator.hasNext()) {
            this.lineIterator.next();
        }
        for (int i = 0; i < columnHandles.size(); i++) {
            S3ColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }
    }

    private AvroColumnDecoder createColumnDecoder(DecoderColumnHandle columnHandle) {
        return new AvroColumnDecoder(columnHandle);
    }

    @Override
    public long getCompletedBytes() {
        return objectInputStream == null
                ? totalBytes // bucket
                : objectInputStream.getTotalBytes();
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
        if(this.schemaname.equals("s3_buckets")) {
            return this.bucketAdvanceNext();
        } else {
            return this.objectAdvanceNext();
        }
    }


    private boolean bucketAdvanceNext() {
        if(!lines.hasNext()) {
            return false;
        }

        Map<String,Object> values = lines.next();

        for (int i = 0; i < columnHandles.size(); i++) {
            final S3ColumnHandle col = this.columnHandles.get(i);
            currentRowValues[i] = new MetadataFieldValueProvider(col, values);
        }

        return true;
    }

    private boolean objectAdvanceNext() {
        if(lineIterator != null && !lineIterator.hasNext()) {
            return false;
        }

        if(byteReader != null && !byteReader.hasNext()) {
            return false;
        }

        if (lineIterator == null && byteReader == null) {
            return false;
        }

        Optional<Map<DecoderColumnHandle, FieldValueProvider>> columnHandleFieldValueProviderMap;
        if (lineIterator != null) {
            String row = lineIterator.next();
            columnHandleFieldValueProviderMap = rowDecoder.decodeRow(row.getBytes(),null);
        } else {
            GenericRecord nextRecord = byteReader.next();

            columnHandleFieldValueProviderMap = Optional.of(AvroColumnDecoders.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().decodeField(nextRecord))));
        }

        if (!columnHandleFieldValueProviderMap.isPresent()) {
            // TODO: https://github.com/EMCECS/presto-s3-connector/issues/25
            return false;
        }

        for (int i = 0; i < columnHandles.size(); i++) {
            currentRowValues[i] = columnHandleFieldValueProviderMap.get().get(columnHandles.get(i));
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
    public void close() {
    }

    static Integer s3SelectColumnMapper(ColumnHandle columnHandle)
    {
        return ((S3ColumnHandle) columnHandle).getAbsoluteSchemaPosition();
    }

    static Type s3SelectTypeMapper(ColumnHandle columnHandle)
    {
        return ((S3ColumnHandle) columnHandle).getType();
    }

    private ArrayList<String> tempS3ObjectToStringObjectList(InputStream inputStream)
    {
        // i don't think can stream with JSONObject. For now read it all.
        ArrayList<String> json = new ArrayList<>();
        String line;
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        try {
            while ((line = bufferedReader.readLine()) != null){
                json.add(line);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return json;
    }
}
