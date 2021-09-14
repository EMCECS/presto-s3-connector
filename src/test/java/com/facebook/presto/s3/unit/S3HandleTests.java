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

package com.facebook.presto.s3.unit;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.block.UncheckedBlock;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import com.facebook.presto.s3.*;

import java.util.*;

import static org.testng.Assert.*;
import static com.facebook.presto.common.type.BigintType.BIGINT;


public class S3HandleTests {

    private final S3ConnectorId connectorId = new S3ConnectorIdTest().s3ConnectorId;
    private final String schemaName = "schemaName";
    private final String schemaName123 = "schemaName123";
    private final String tableName = "tableName";
    private final String tableBucketName = "testbucket";
    private final String tableBucketPrefix = "TestData/";
    private final String objectDataFormat = "CSV";
    private final String has_header_row = "false";
    private final String record_delimiter = "\n";
    private final String field_delimiter = ",";
    private final List<String> objects = Arrays.asList("source1", "source2");
    private final Map<String, List<String>> objectBucketMap = Collections.singletonMap("bucket", objects);

    // for use in S3SplitTest
    public final S3TableHandle escTableHandle = new S3TableHandle(
            connectorId.toString(),
            schemaName,
            tableName,
            objectDataFormat,
            field_delimiter,
            record_delimiter,
            has_header_row,
            tableBucketName,
            tableBucketPrefix,
            objectBucketMap);

    public final S3TableHandle escTableHandle1 = new S3TableHandle(
            connectorId.toString(),
            schemaName123,
            tableName,
            objectDataFormat,
            field_delimiter,
            record_delimiter,
            has_header_row,
            tableBucketName,
            tableBucketPrefix,
            objectBucketMap);

    public final S3TableHandle escTableHandle2 = new S3TableHandle(
            connectorId.toString(),
            schemaName,
            tableName,
            objectDataFormat,
            field_delimiter,
            record_delimiter,
            has_header_row,
            tableBucketName,
            tableBucketPrefix,
            objectBucketMap);

    @Test
    public void testGetConnectorId() {
        assertEquals(escTableHandle.getConnectorId(), connectorId.toString());
    }

    @Test
    public void testGetSchemaName() {
        assertEquals(escTableHandle.getSchemaName(), schemaName);
    }

    @Test
    public void testGetTableName() {
        assertEquals(escTableHandle.getTableName(), tableName);
    }

    @Test
    public void testEquals() {
        assertTrue(escTableHandle.equals(escTableHandle2));
    }

    @Test
    public void testNotEquals() {
        assertFalse(escTableHandle.equals(escTableHandle1));
    }

    @Test
    public void testGetBucketObjectsMap() {
    }

    @Test
    public void testToSchemaTableName() {
    }

    @Test
    public void testInsertTableHandle() {
        List<String> columnNames = new ArrayList<>();
        columnNames.add("Col1");
        columnNames.add("Col2");
        List<Type> columnTypes = new ArrayList<>();
        columnTypes.add(BIGINT);
        columnTypes.add(BIGINT);
        S3InsertTableHandle insertTableHandle = new S3InsertTableHandle(
                        connectorId.toString(),
                        schemaName,
                        tableName,
                        columnNames,
                        columnTypes,
                        tableBucketName,
                        tableBucketPrefix,
                        has_header_row,
                        record_delimiter,
                        field_delimiter,
                        objectDataFormat);

        assertTrue(insertTableHandle.getConnectorId().equalsIgnoreCase(connectorId.toString()));
        assertTrue(insertTableHandle.getSchemaName().equalsIgnoreCase(schemaName));
        assertTrue(insertTableHandle.getTableBucketName().equalsIgnoreCase(tableBucketName));
        assertTrue(insertTableHandle.getTableBucketPrefix().equalsIgnoreCase(tableBucketPrefix));
        assertTrue(insertTableHandle.getTableName().equalsIgnoreCase(tableName));
        assertEquals(insertTableHandle.getColumnNames().size(),2);
        assertEquals(insertTableHandle.getColumnTypes().size(),2);
        assertTrue(insertTableHandle.getObjectDataFormat().equalsIgnoreCase(objectDataFormat));
    }

    @Test
    public void testOututTableHandle() {
        String value1 = "value1";
        SchemaTableName stn = new SchemaTableName(schemaName, tableName);
        List<S3Column> s3Columns = new ArrayList<>();
        S3Column col1 = new S3Column("Col1", BIGINT, null);
        S3Column col2 = new S3Column("Col2", BIGINT, null);
        s3Columns.add(col1);
        s3Columns.add(col2);
        Map<String, Object> properties = new HashMap<>();
        properties.put("property1", value1);

        S3OutputTableHandle outputTableHandle = new S3OutputTableHandle(
                connectorId.toString(),
                stn,
                properties,
                s3Columns);
        assertTrue(outputTableHandle.getSchemaTableName().getSchemaName().equalsIgnoreCase(schemaName));
        assertTrue(outputTableHandle.getSchemaTableName().getTableName().equalsIgnoreCase(tableName));
        assertEquals(outputTableHandle.getColumns().size(), 2);
    }
}
