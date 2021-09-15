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
package com.facebook.presto.s3.decoder;

import com.facebook.presto.s3.S3ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slices;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static org.testng.Assert.*;

@Test
public class MetadataFieldValueProviderTest
{
    private final Map<String, Object> valueMap = new HashMap<>();

    @BeforeSuite
    public void beforeSuite()
    {
        valueMap.put("booleanfield", "true");
        valueMap.put("longfield", "27");
        valueMap.put("doublefield", "27.89");
        valueMap.put("slicefield", "john smith");
        valueMap.put("nullfield", null);
    }

    @Test
    public void testBoolean()
    {
        assertTrue(new MetadataFieldValueProvider(column("booleanField"), valueMap).getBoolean());
    }

    @Test
    public void testLong()
    {
        assertEquals(new MetadataFieldValueProvider(column("longField"), valueMap).getLong(), 27L);
    }

    @Test
    public void testDouble()
    {
        assertEquals(new MetadataFieldValueProvider(column("doubleField"), valueMap).getDouble(), 27.89);
    }

    @Test
    public void testSlice()
    {
        assertEquals(new MetadataFieldValueProvider(column("sliceField"), valueMap).getSlice(), Slices.utf8Slice("john smith"));
    }

    @Test
    public void testIsNull()
    {
        assertTrue(new MetadataFieldValueProvider(column("nullField"), valueMap).isNull());
        assertTrue(new MetadataFieldValueProvider(column("fieldDoesNotExist"), valueMap).isNull());
    }

    @Test(expectedExceptions = {PrestoException.class})
    public void testBlock()
    {
        new MetadataFieldValueProvider(column("blockField"), valueMap).getBlock();
    }

    private S3ColumnHandle column(final String name)
    {
        // only name is used in this testing path
        return new S3ColumnHandle("unused-connectorId",
                0,
                name,
                BIGINT,
                "unused-mapping",
                "unused-dataFormat",
                "unused-formatHint",
                false /* keyDecoder */,
                false /* hidden */,
                false /* internal */);
    }
}
