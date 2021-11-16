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

import com.facebook.presto.s3.S3Column;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;


import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.DateType.DATE;

import static org.testng.Assert.assertEquals;

public class S3ColumnTest {

    private final S3Column column1 = new S3Column(
            "column1Name",
            BIGINT,
            null

    );

    private final S3Column column2 = new S3Column(
            "column2Name",
            VARCHAR,
            null
    );

    private final S3Column column3 = new S3Column(
            "column3Name",
            DATE,
            null
    );

    private final S3Column column4 = new S3Column(
            "column1Name",
            BIGINT,
            null
    );

    // for use in S3TableTest
    public final List<S3Column> columnsList = ImmutableList.of(column1, column2, column3);

    @Test
    public void testGetName() {
        assertEquals(column1.getName(), "column1Name");
        assertEquals(column2.getName(), "column2Name");
        assertEquals(column3.getName(), "column3Name");
    }

    @Test
    public void testGetType() {
        assertEquals(column1.getType(), BIGINT);
        assertEquals(column2.getType(), VARCHAR);
        assertEquals(column3.getType(), DATE);
    }

    @Test
    public void testColumnEquals() {
        assertEquals(column4.equals(column4), true);
        assertEquals(column4.equals(null), false);
    }
}
