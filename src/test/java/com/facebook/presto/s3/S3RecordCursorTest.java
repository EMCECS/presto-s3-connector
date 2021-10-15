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
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.decoder.json.JsonRowDecoderFactory;
import com.facebook.presto.s3.avro.User;
import com.facebook.presto.s3.reader.AvroRecordReader;
import com.facebook.presto.s3.reader.CsvRecordReader;
import com.facebook.presto.s3.reader.JsonRecordReader;
import com.facebook.presto.s3.reader.RecordReader;
import com.facebook.presto.s3.util.AvroByteArrayOutputStream;
import com.facebook.presto.s3.util.SeekableByteArrayInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.testng.annotations.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.function.Supplier;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.*;

public class S3RecordCursorTest {

    private static final int AVRO_BLOCK_SIZE_BYTES = 16*1024;

    /*
     * start test helpers
     */
    static class ColumnBuilder {

        int position = 0;

        ImmutableList.Builder<S3ColumnHandle> builder = ImmutableList.builder();

        ColumnBuilder add(final String name, final Type type) {
            builder.add(column(name, type, position++, null));
            return this;
        }

        ColumnBuilder add(final String name, final String mapping, final Type type) {
            builder.add(column(name, type, position++, mapping));
            return this;
        }

        List<S3ColumnHandle> build() {
            return builder.build();
        }

        S3ColumnHandle column(final String name, final Type type, int position, String mapping)
        {
            return new S3ColumnHandle("s3",
                    position,
                    name,
                    type,
                    mapping != null ? mapping : String.valueOf(position),
                    null /* dataFormat */,
                    null /* formatHint */,
                    false /* keyDecoder */,
                    false /* hidden */,
                    false /* internal */);
        }
    }

    S3TableLayoutHandle table(String dataFormat) {
        // csv, only field+record delim, and header row used
        S3TableHandle table = new S3TableHandle("s3",
                "schema",
                "table",
                dataFormat,
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

    RecordReader newCsvFileReader(List<S3ColumnHandle> columns, String f) {
        return newFileReader(columns, f, S3Const.CSV);
    }

    RecordReader newFileReader(List<S3ColumnHandle> columns, String f, String dataFormat) {
        switch (dataFormat) {
            case S3Const.CSV:
                return new CsvRecordReader(columns,
                        new S3ObjectRange("bucket", "key"),
                        table(dataFormat),
                        new S3ReaderProps(false, 65536),
                        readerStream(f));
            case S3Const.JSON:
                RowDecoder rowDecoder =
                        new JsonRowDecoderFactory(new ObjectMapper()).create(ImmutableMap.of(), new HashSet<>(columns));
                return new JsonRecordReader(rowDecoder,
                        new S3ObjectRange("bucket", "key", 0, (int) new File(f).length()),
                        new S3ReaderProps(false, 65536),
                        readerStream(f));
            default:
        throw new UnsupportedOperationException();
        }

    }

    RecordReader newCsvStringReader(List<S3ColumnHandle> columns, String streamAsString) {
        return newStringReader(columns, streamAsString, S3Const.CSV);
    }

    RecordReader newStringReader(List<S3ColumnHandle> columns, String streamAsString, String dataFormat) {
        Supplier<CountingInputStream> stream =
                readerStream(new ByteArrayInputStream(streamAsString.getBytes(StandardCharsets.UTF_8)));

        switch (dataFormat) {
            case S3Const.CSV:
                return new CsvRecordReader(columns,
                        new S3ObjectRange("bucket", "key"),
                        table(dataFormat),
                        new S3ReaderProps(false, 65536),
                        stream);

            case S3Const.JSON:
                RowDecoder rowDecoder =
                        new JsonRowDecoderFactory(new ObjectMapper()).create(ImmutableMap.of(), new HashSet<>(columns));
                return new JsonRecordReader(rowDecoder,
                        // 0-34
                        // 35-70

                        // S3ObjectRange start-end must contain at least 1 full record

                        new S3ObjectRange("bucket", "key", 1, 40),
                        new S3ReaderProps(false, 65536),
                        stream);
            default:
                throw new UnsupportedOperationException();
        }
    }


    RecordReader newAvroRecordReader(List<S3ColumnHandle> columns, byte[] bytes, int start, int end) {
        final SeekableByteArrayInputStream byteArrayInputStream = new SeekableByteArrayInputStream(bytes);
        InputStream inputStream = new FSDataInputStream(
                new BufferedFSInputStream(
                        new FSInputStream() {
                            @Override
                            public void seek(long l) {
                                byteArrayInputStream.seek(l);
                            }

                            @Override
                            public long getPos() {
                                return byteArrayInputStream.tell();
                            }

                            @Override
                            public boolean seekToNewSource(long l) {
                                return false;
                            }

                            @Override
                            public int read() {
                                return byteArrayInputStream.read();
                            }

                            @Override
                            public int read(byte[] b, int off, int len) {
                                return byteArrayInputStream.read(b, off, len);
                            }
                        },
                        65536));

            return new AvroRecordReader(columns,
                    new S3ObjectRange("bucket", "key", start, end-start),
                    readerStream(inputStream));
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
                new S3RecordCursor(newCsvFileReader(columnHandles, "emptyline.csv"), columnHandles);

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
                new S3RecordCursor(newCsvStringReader(columnHandles, line), columnHandles);

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getLong(0), 1027L);
        assertTrue(cursor.getBoolean(1));
    }

    @Test
    public void testJson() {
        List<S3ColumnHandle> columnHandles =
                new ColumnBuilder()
                        .add("field1", "field1", VARCHAR)
                        .add("field2", "field2", BOOLEAN)
                        .build();

        String line = "{\"field1\": \"james\",\"field2\": true}\n" +
                "{\"field1\": \"andrew\",\"field2\": false}\n" +
                "{\"field1\": \"karan\",\"field2\": true}";

        S3RecordCursor cursor =
                new S3RecordCursor(newStringReader(columnHandles, line, S3Const.JSON), columnHandles);

        /*
        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getSlice(0).toStringUtf8(), "james");
        assertTrue(cursor.getBoolean(1));
        */

        assertTrue(cursor.advanceNextPosition());
        assertEquals(cursor.getSlice(0).toStringUtf8(), "andrew");
        assertFalse(cursor.getBoolean(1));

        assertFalse(cursor.advanceNextPosition());
    }


    @Test
    public void testAvroSplits() throws Exception {
        List<S3ColumnHandle> columnHandles =
                new ColumnBuilder()
                        .add("userId", "userId", BIGINT)
                        .add("first", "first", VARCHAR)
                        .add("last", "last", VARCHAR)
                        .build();

        // write a few blocks worth of avro records
        // userId field value 0->N so that we can ensure come back in order

        User user = User.newBuilder().setUserId(0).setFirst("f").setLast("l").build();
        DatumWriter<User> sampleDatumWriter = new SpecificDatumWriter<>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<>(sampleDatumWriter);
        dataFileWriter.setSyncInterval(AVRO_BLOCK_SIZE_BYTES);

        // generate our own sync so we can look for+capture it
        byte[] sync = new byte[] {
                'a', 'v', 'r', 'o', 's', 'y', 'n', 'c', 0, 1, 2, 3, 4, 5, 6, 7
        };

        AvroByteArrayOutputStream os = new AvroByteArrayOutputStream(sync);
        dataFileWriter.create(user.getSchema(), os, sync);

        int avroBlocksToWrite = 3;

        int userId = 0;
        while (os.getSyncs() < avroBlocksToWrite) {
            user = User.newBuilder()
                    .setUserId(userId)
                    .setFirst("first_" + userId)
                    .setLast("last_" + userId).build();
            dataFileWriter.append(user);
            userId++;
        }
        dataFileWriter.close();

        // more like 2k but sanity check that we wrote something
        assertTrue(userId > 1000);
        assertEquals(avroBlocksToWrite, os.getSyncs());

        // read entire object back using different splits
        // each split will use different offset, length in the data

        int records = 0;
        int n;
        int split = 0;
        int splitsWithData = 0;

        // collect each id from the data to ensure that different splits don't return same data
        HashSet<Long> duplicateSet = new HashSet<>();

        byte[] avroData = os.toByteArray();
        do {
            n = readSplit(columnHandles, duplicateSet, avroData, split++);
            records += n;
            System.out.println("split " + (split-1) + " returned " + n + " records");
            if (n > 0) {
                splitsWithData++;
            }
        } while (n > 0);

        assertEquals(avroBlocksToWrite, splitsWithData); // read all expected blocks
        assertEquals(split - 1, splitsWithData);      // only 1 empty split (the last)
        assertEquals(userId, records);                  // expected number of records

        System.out.println("read " + records + " records in total from " + split + " splits");
    }

    private int readSplit(List<S3ColumnHandle> columnHandles, HashSet<Long> duplicateSet, byte[] avroData, int splitNum) {
        S3RecordCursor cursor =
                new S3RecordCursor(newAvroRecordReader(columnHandles, avroData,
                        AVRO_BLOCK_SIZE_BYTES * splitNum,
                        AVRO_BLOCK_SIZE_BYTES * splitNum + AVRO_BLOCK_SIZE_BYTES), columnHandles);
        int records = 0;
        long lastId = -1;
        while (cursor.advanceNextPosition()) {
            // check that the id hasn't been returned before
            // and that always increasing in sequence
            assertTrue(duplicateSet.add(cursor.getLong(0)));
            if (lastId >= 0) {
                assertEquals(lastId+1, cursor.getLong(0));
            }
            lastId = lastId == -1 ? cursor.getLong(0) : lastId + 1;
            records++;
        }
        return records;
    }
}

