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
 * This file is from prestodb Hive Connector
 * https://github.com/prestodb/presto/blob/ca09cd31fedba77611975d5717cbaae80d2e984b/presto-hive/src/test/java/com/facebook/presto/hive/TestIonSqlQueryBuilder.java
 */

package com.facebook.presto.s3.integration;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.s3.IonSqlQueryBuilder;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.common.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestIonSqlQueryBuilder {
    private static class TestColumnHandle
            implements ColumnHandle {
        final int order;
        final Type type;

        TestColumnHandle(Type type, int order) {
            this.type = type;
            this.order = order;
        }
    }

    static Integer columnIndex(ColumnHandle columnHandle) {
        return ((TestColumnHandle) columnHandle).order;
    }

    static Type columnType(ColumnHandle columnHandle) {
        return ((TestColumnHandle) columnHandle).type;
    }

    static ColumnHandle newHandle(Type type, int order) {
        return new TestColumnHandle(type, order);
    }

    @Test
    public void testBuildSQL() {
        List<ColumnHandle> columns = ImmutableList.of(
                newHandle(INTEGER, 0),
                newHandle(VARCHAR, 1),
                newHandle(INTEGER, 2));

        assertEquals("SELECT s._1, s._2, s._3 FROM S3Object s", buildSql(columns, TupleDomain.all()));
        TupleDomain<ColumnHandle> tupleDomain = withColumnDomains(ImmutableMap.of(
                columns.get(2), Domain.create(SortedRangeSet.copyOf(BIGINT, ImmutableList.of(Range.equal(BIGINT, 3L))), false)));
        assertEquals("SELECT s._1, s._2, s._3 FROM S3Object s WHERE (case s._3 when '' then null else CAST(s._3 AS INT) end = 3)",
                buildSql(columns, tupleDomain));
    }

    @Test
    public void testEmptyColumns() {
        assertEquals("SELECT ' ' FROM S3Object s", buildSql(ImmutableList.of(), TupleDomain.all()));
    }

    private static String buildSql(List<? extends ColumnHandle> columns,
                                   TupleDomain<ColumnHandle> tupleDomain) {
        return new IonSqlQueryBuilder().buildSql(TestIonSqlQueryBuilder::columnIndex,
                TestIonSqlQueryBuilder::columnType,
                columns,
                tupleDomain);
    }
}
