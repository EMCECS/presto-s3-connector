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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.s3.reader.CsvRecordReader;
import com.facebook.presto.s3.reader.RecordReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Supplier;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.*;

public class S3RecordCursorTest {

    // note: helpers support CSV only.  though only need some tweaks for other formats.

    /*
     * start test helpers
     */
    static class ColumnBuilder {

        int position = 0;

        ImmutableList.Builder<S3ColumnHandle> builder = ImmutableList.builder();

        ColumnBuilder add(final String name, final Type type) {
            builder.add(column(name, type, position++));
            return this;
        }

        List<S3ColumnHandle> build() {
            return builder.build();
        }

        S3ColumnHandle column(final String name, final Type type, int position)
        {
            return new S3ColumnHandle("s3",
                    position,
                    name,
                    type,
                    String.valueOf(position),
                    "unused-dataFormat",
                    "unused-formatHint",
                    false /* keyDecoder */,
                    false /* hidden */,
                    false /* internal */);
        }
    }

    S3TableLayoutHandle table() {
        // csv, only field+record delim, and header row used
        S3TableHandle table = new S3TableHandle("s3",
                "schema",
                "table",
                S3Const.CSV,
                "," /* field delim */,
                "\n" /* record delim */,
                "false" /* header row */,
                "",
                "",
                ImmutableMap.of());
        return new S3TableLayoutHandle(table, null);
    }

    Supplier<CountingInputStream> readerStream(String f) {
        return readerStream(S3RecordCursorTest.class.getResourceAsStream("/cursor/" + f));
    }

    Supplier<CountingInputStream> readerStream(InputStream stream) {
        return () -> new CountingInputStream(stream);
    }

    RecordReader newFileReader(List<S3ColumnHandle> columns, String f) {
        return new CsvRecordReader(columns,
                new S3ObjectRange("bucket", "key"),
                table(),
                new S3ReaderProps(false, 65536),
                readerStream(f));
    }

    RecordReader newStringReader(List<S3ColumnHandle> columns, String streamAsString) {
        return new CsvRecordReader(columns,
                new S3ObjectRange("bucket", "key"),
                table(),
                new S3ReaderProps(false, 65536),
                readerStream(new ByteArrayInputStream(streamAsString.getBytes(StandardCharsets.UTF_8))));
    }

    /*
     * end test helpers
     */


    /*
     * begin tests
     */

    @Test
    public void testCsv() {
        List<S3ColumnHandle> columnHandles =
                new ColumnBuilder()
                        .add("field1", VARCHAR)
                        .add("field2", BIGINT)
                        .build();

        S3RecordCursor cursor =
                new S3RecordCursor(newFileReader(columnHandles, "emptyline.csv"), columnHandles);

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getSlice(0).toStringUtf8(), "andrew");
        assertEquals(cursor.getLong(1), 27L);

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getSlice(0).toStringUtf8(), "tim");
        assertEquals(cursor.getLong(1), 11L);

        assertTrue(cursor.advanceNextPosition());
        assertTrue(cursor.isNull(0));
        assertTrue(cursor.isNull(1));

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getSlice(0).toStringUtf8(), "xavier");
        assertEquals(cursor.getLong(1), 33L);

        assertFalse(cursor.advanceNextPosition());
    }

    @Test
    public void testQuotedTypes() {
        List<S3ColumnHandle> columnHandles =
                new ColumnBuilder()
                        .add("field1", BIGINT)
                        .add("field2", BOOLEAN)
                        .build();

        String line = "\"1027\",\"true\"";

        S3RecordCursor cursor =
                new S3RecordCursor(newStringReader(columnHandles, line), columnHandles);

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getLong(0), 1027L);
        assertTrue(cursor.getBoolean(1));
    }
}

