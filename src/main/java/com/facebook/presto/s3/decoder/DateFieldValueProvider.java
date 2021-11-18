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
 *
 * Note: This class contains code from prestodb
 * (commit hash 80c84566a8dde221cfb4e6be911df9168254f27c)
 * https://github.com/prestodb/presto/blob/80c84566a8dde221cfb4e6be911df9168254f27c/presto-record-decoder/src/main/java/com/facebook/presto/decoder/json/ISO8601JsonFieldDecoder.java
 */
package com.facebook.presto.s3.decoder;

import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.s3.S3ColumnHandle;

import java.time.temporal.TemporalAccessor;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static java.time.format.DateTimeFormatter.*;
import static java.time.temporal.ChronoField.EPOCH_DAY;
import static java.time.temporal.ChronoField.MILLI_OF_DAY;

public class DateFieldValueProvider
        extends FieldValueProvider {
    private final CsvRecord record;

    private final S3ColumnHandle columnHandle;

    private final int field;

    public DateFieldValueProvider(CsvRecord record, S3ColumnHandle columnHandle) {
        this.record = record;
        this.columnHandle = columnHandle;
        this.field = columnHandle.getOrdinalPosition();
    }

    @Override
    public long getLong() {
        String s = record.getSlice(field).toStringUtf8();

        if (columnHandle.getType() == DATE) {
            return ISO_DATE.parse(s).getLong(EPOCH_DAY);
        } else if (columnHandle.getType() == TIMESTAMP) {
            TemporalAccessor parseResult = ISO_DATE_TIME.parse(s);
            return TimeUnit.DAYS.toMillis(parseResult.getLong(EPOCH_DAY)) + parseResult.getLong(MILLI_OF_DAY);
        } else if (columnHandle.getType() == TIME) {
            return ISO_TIME.parse(s).getLong(MILLI_OF_DAY);
        } else {
            throw new IllegalArgumentException("invalid or unhandled type " + columnHandle);
        }
    }

    @Override
    public boolean isNull() {
        return record.isNull(field);
    }
}
