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

import com.facebook.presto.spi.ColumnMetadata;
import org.testng.annotations.Test;

import com.facebook.presto.s3.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import static org.testng.Assert.assertEquals;

public class S3TableTest {
    private final String name = "name";
    private final List<S3Column> columnsList = new S3ColumnTest().columnsList;
    private final String objectDataFormat = "objectFormat";
    private final String hasHeaderRow = "false";
    private final String recordDelimiter = "\n";
    private final String fieldDelimiter = "\t";
    private final String tableBucketName = "testbucket";
    private final String tableBucketPrefix = "TestData/";
    private final List<String> objects = Arrays.asList("source1", "source2");
    private final Map<String, List<String>> objectBucketMap = Collections.singletonMap("bucket", objects);

    // for use in S3TableHandleTest
    public final S3Table table = new S3Table(
            name,
            columnsList,
            objectDataFormat,
            hasHeaderRow,
            recordDelimiter,
            fieldDelimiter,
            tableBucketName,
            tableBucketPrefix,
            objectBucketMap);

    @Test
    public void testGetName() {
        assertEquals(table.getName(), "name");
    }

    @Test
    public void testGetObjectDataFormat() {
        assertEquals(table.getObjectDataFormat(), "objectFormat");
    }

    @Test
    public void testGetColumns() {
        assertEquals(table.getColumns(), new S3ColumnTest().columnsList);
    }

    @Test
    public void testGetColumnsMetadata() {
        List<ColumnMetadata> columnsMetadata = new ArrayList<>();
        for (S3Column column : columnsList) {
            columnsMetadata.add(new ColumnMetadata(column.getName(), column.getType()));
        }
        assertEquals(table.getColumnsMetadata(), columnsMetadata);
    }

    @Test
    public void testGetBucketObjectsMap() {
        assertEquals(table.getBucketObjectsMap(), Collections.singletonMap("bucket", objects));
    }

    @Test
    public void testGetHasHeaderRow() {
        assertEquals(table.getHasHeaderRow(), "false");
    }

    @Test
    public void testGetFieldDelimiter() {
        assertEquals(table.getFieldDelimiter(), "\t");
    }
}
