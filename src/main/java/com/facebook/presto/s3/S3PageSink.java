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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.opencsv.CSVWriter;
import com.opencsv.ICSVWriter;
import io.airlift.slice.Slice;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.facebook.airlift.log.Logger;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static com.facebook.presto.s3.S3Const.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class S3PageSink
        implements ConnectorPageSink {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneId.of("UTC"));
    private static final Logger log = Logger.get(com.facebook.presto.s3.S3PageSink.class);
    private final S3AccessObject accessObject;
    private final List<Type> columnTypes;
    private final List<String> columnNames;
    private final boolean generateUUID;
    private final String schemaName;
    private final String tableName;
    private final String tableBucketName;
    private String tableBucketPrefix;
    private final String file_format;
    private final String has_header_row;
    private final String record_delimiter;
    private final String field_delimiter;

    public S3PageSink(
            String schemaName,
            String tableName,
            List<String> columnNames,
            List<Type> columnTypes,
            String tableBucketName,
            String tableBucketPrefix,
            String file_format,
            String has_header_row,
            String record_delimiter,
            String field_delimiter,
            boolean generateUUID,
            S3AccessObject accessObject) {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        this.columnNames = requireNonNull(columnNames, "columnNames is null");
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        this.generateUUID = generateUUID;
        this.accessObject = accessObject;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableBucketName = tableBucketName;
        this.tableBucketPrefix = tableBucketPrefix;
        this.file_format = file_format;
        this.has_header_row = has_header_row;
        this.record_delimiter = record_delimiter;
        this.field_delimiter = field_delimiter;

        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            checkArgument(columnName != null, "columnName is null at position: %d", i);
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page) {

        if (file_format.equalsIgnoreCase(S3Const.CSV)) {
            return appendCSVPage(page, file_format);
        } else if (file_format.equalsIgnoreCase(JSON)) {
            return appendJsonPage(page, file_format);
        } else {
            throw new PrestoException(S3ErrorCode.S3_UNSUPPORTED_FORMAT, format("File format %s is not supported", file_format));
        }
    }

    private CompletableFuture<?> appendJsonPage(Page page, String file_format) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (int position = 0; position < page.getPositionCount(); position++) {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode rootNode = mapper.createObjectNode();
            if (generateUUID) {
                rootNode.put("UUID", String.valueOf(UUID.randomUUID()));
            }

            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                populateJsonObjects(rootNode, page, position, channel);
            }
            try {
                String newLine = System.getProperty("line.separator");
                mapper.writeValue(baos, rootNode);
                for (int i = 0; i < newLine.length(); ++i) {
                    baos.write(newLine.charAt(i));
                }
            } catch (IOException e) {
                System.out.println("Error while writing object node to output stream");
                e.printStackTrace();
            }
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());

        if (tableBucketPrefix.startsWith("/")) {
            tableBucketPrefix = tableBucketPrefix.replaceFirst("/", "");
        }
        accessObject.putObject(tableName, tableBucketName, tableBucketPrefix, bais, file_format);
        return NOT_BLOCKED;
    }

    private void populateJsonObjects(ObjectNode jsonMap, Page page, int position, int channel) {
        Block block = page.getBlock(channel);
        Type type = columnTypes.get(channel);
        String name = columnNames.get(channel);
        if (block.isNull(position)) {
            jsonMap.put(null, (byte[]) null);
        } else if (BOOLEAN.equals(type)) {
            jsonMap.put(name, type.getBoolean(block, position));
        } else if (BIGINT.equals(type)) {
            jsonMap.put(name, type.getLong(block, position));
        } else if (INTEGER.equals(type)) {
            jsonMap.put(name, toIntExact(type.getLong(block, position)));
        } else if (DOUBLE.equals(type)) {
            jsonMap.put(name, type.getDouble(block, position));
        } else if (REAL.equals(type)) {
            jsonMap.put(name, intBitsToFloat(toIntExact(type.getLong(block, position))));
        } else if (DATE.equals(type)) {
            jsonMap.put(name, DATE_FORMATTER.format(Instant.ofEpochMilli(TimeUnit.DAYS.toMillis(type.getLong(block, position)))));
        } else if (TIME.equals(type)) {
            jsonMap.put(name, String.valueOf(new Time(type.getLong(block, position))));
        } else if (TIMESTAMP.equals(type)) {
            jsonMap.put(name, String.valueOf(new Timestamp(type.getLong(block, position))));
        } else if (isVarcharType(type)) {
            jsonMap.put(name, type.getSlice(block, position).toStringUtf8());
        } else if (VARBINARY.equals(type)) {
            jsonMap.put(name, String.valueOf(type.getSlice(block, position).toByteBuffer()));
        } else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
    }

    private CompletableFuture<?> appendCSVPage(Page page, String file_format) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStreamWriter osw = new OutputStreamWriter(baos);
        CSVWriter csvWriter = new CSVWriter(osw, field_delimiter.charAt(0),
                ICSVWriter.DEFAULT_QUOTE_CHARACTER,
                ICSVWriter.NO_ESCAPE_CHARACTER, record_delimiter);

        List<String[]> inputPage = new ArrayList<String[]>();
        if (has_header_row.equalsIgnoreCase(TRUE)) {
            List<String> headerRow = new ArrayList<>(columnNames.size() + 1);
            if (generateUUID) {
                headerRow.add("UUID");
            }
            for (int i = 0; i < columnNames.size(); i++) {
                String columnName = columnNames.get(i);
                headerRow.add(columnName);
            }
            String[] row = new String[headerRow.size()];
            row = headerRow.toArray(row);
            inputPage.add(row);
        }
        for (int position = 0; position < page.getPositionCount(); position++) {
            List<Object> values = new ArrayList<>(columnTypes.size() + 1);
            if (generateUUID) {

                values.add(UUID.randomUUID());
            }

            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                appendColumn(values, page, position, channel);
            }

            List<String> ls = Lists.transform(values, Functions.toStringFunction());
            String[] row = new String[ls.size()];
            row = ls.toArray(row);
            inputPage.add(row);
        }
        csvWriter.writeAll(inputPage, false);
        try {
            csvWriter.flush();
        } catch (Exception e) {
            log.error("Flush error: " + e);
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());

        if (tableBucketPrefix.startsWith("/")) {
            tableBucketPrefix = tableBucketPrefix.replaceFirst("/", "");
        }
        accessObject.putObject(tableName, tableBucketName, tableBucketPrefix, bais, file_format);

        return NOT_BLOCKED;
    }

    private void appendColumn(List<Object> values, Page page, int position, int channel) {
        Block block = page.getBlock(channel);
        Type type = columnTypes.get(channel);
        if (block.isNull(position)) {
            values.add(null);
        } else if (BOOLEAN.equals(type)) {
            values.add(type.getBoolean(block, position));
        } else if (BIGINT.equals(type)) {
            values.add(type.getLong(block, position));
        } else if (INTEGER.equals(type)) {
            values.add(toIntExact(type.getLong(block, position)));
        } else if (DOUBLE.equals(type)) {
            values.add(type.getDouble(block, position));
        } else if (REAL.equals(type)) {
            values.add(intBitsToFloat(toIntExact(type.getLong(block, position))));
        } else if (DATE.equals(type)) {
            values.add(DATE_FORMATTER.format(Instant.ofEpochMilli(TimeUnit.DAYS.toMillis(type.getLong(block, position)))));
        } else if (TIME.equals(type)) {
            values.add(new Time(type.getLong(block, position)));
        } else if (TIMESTAMP.equals(type)) {
            values.add(new Timestamp(type.getLong(block, position)));
        } else if (isVarcharType(type)) {
            values.add(type.getSlice(block, position).toStringUtf8());
        } else if (VARBINARY.equals(type)) {
            values.add(type.getSlice(block, position).toByteBuffer());
        } else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish() {
        // the committer does not need any additional info
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort() {
    }
}
