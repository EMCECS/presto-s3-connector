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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.s3.*;
import com.facebook.presto.s3.decoder.CsvFieldValueProvider;
import com.facebook.presto.spi.ColumnHandle;

import java.io.Closeable;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.s3.S3Const.*;

public class CsvRecordReader
        implements RecordReader, Closeable
{
    private static final Logger log = Logger.get(CsvRecordReader.class);

    private final List<S3ColumnHandle> columnHandles;

    private final S3AccessObject accessObject;

    private final S3ObjectRange objectRange;

    private final S3TableLayoutHandle table;

    private final S3ReaderProps readerProps;

    private final S3RecordImpl record;

    private BytesLineReader lineReader = null;

    private boolean haveRow = false;

    private final Map<DecoderColumnHandle, FieldValueProvider> row;

    // TODO: make sure we eat header row properly
    public CsvRecordReader(List<S3ColumnHandle> columnHandles,
                           S3AccessObject accessObject,
                           S3ObjectRange objectRange,
                           S3TableLayoutHandle table,
                           S3ReaderProps readerProps)
    {
        if (readerProps.getS3SelectEnabled()) {
            // setting s3SelectPushdownEnabled currently checks format = csv - sanity check that here
            if (!table.getTable().getObjectDataFormat().equals(CSV) &&
                    !table.getTable().getObjectDataFormat().equals(TEXT)) {
                throw new IllegalArgumentException("s3SelectPushdownEnabled for non delim file");
            }
        }

        if (table.getTable().getFieldDelimiter().length() != 1) {
            throw new IllegalArgumentException(table.getTable().getFieldDelimiter());
        }

        this.columnHandles = columnHandles;
        this.accessObject = accessObject;
        this.objectRange = objectRange;
        this.table = table;
        this.readerProps = readerProps;

        this.record = new S3RecordImpl(table.getTable().getFieldDelimiter().charAt(0));

        // create col->value provider objects once as all the same underlying objects are used (i.e. record)
        this.row = new HashMap<>();
        for (S3ColumnHandle columnHandle : columnHandles) {
            this.row.put(columnHandle, new CsvFieldValueProvider(record, columnHandle.getOrdinalPosition()));
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
                table.getTable().getHasHeaderRow().equals(LC_TRUE)) {
            // eat the header
            lineReader.read(record.value);
        }
    }

    private InputStream objectStream()
    {
        if (readerProps.getS3SelectEnabled()) {
            String sql = new IonSqlQueryBuilder()
                    .buildSql(CsvRecordReader::s3SelectColumnMapper,
                            CsvRecordReader::s3SelectTypeMapper,
                            columnHandles,
                            table.getConstraints());
            log.info("s3select " + objectRange + ", " + sql);
            final boolean hasHeaderRow = table.getTable().getHasHeaderRow().equals(LC_TRUE);
            final String recordDelimiter = table.getTable().getRecordDelimiter();
            final String fieldDelimiter = table.getTable().getFieldDelimiter();

            return accessObject.selectObjectContent(objectRange, sql,
                    new S3SelectProps(hasHeaderRow, recordDelimiter, fieldDelimiter));
        }
        else {
            return accessObject.getObject(objectRange.getBucket(), objectRange.getKey(), objectRange.getOffset());
        }
    }

    @Override
    public boolean hasNext()
    {
        if (haveRow) {
            return true;
        }
        return advance();
    }

    private boolean advance()
    {
        if (lineReader == null) {
            init();
        }

        record.len = lineReader.read(record.value);
        record.decoded = false;
        haveRow = record.len > 0;
        return haveRow;
    }

    @Override
    public Map<DecoderColumnHandle, FieldValueProvider> next()
    {
        if (!haveRow) {
            if (!advance()) {
                return null;
            }
        }
        haveRow = false;
        return row;
    }

    static Integer s3SelectColumnMapper(ColumnHandle columnHandle)
    {
        return ((S3ColumnHandle) columnHandle).getAbsoluteSchemaPosition();
    }

    static Type s3SelectTypeMapper(ColumnHandle columnHandle)
    {
        return ((S3ColumnHandle) columnHandle).getType();
    }

    @Override
    public void close()
    {
        if (lineReader != null) {
            lineReader.close();
        }
    }
}
