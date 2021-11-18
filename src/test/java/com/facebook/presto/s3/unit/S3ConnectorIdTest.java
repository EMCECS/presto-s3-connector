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

import com.facebook.presto.s3.S3ConnectorId;
import org.testng.annotations.Test;

import java.util.Objects;

import static org.testng.Assert.*;

public class S3ConnectorIdTest {
    private final String id = "connectorId";
    private final String id1 = "connectorId1";
    private final String id2 = "connectorId";

    public final S3ConnectorId s3ConnectorId = new S3ConnectorId(id);

    public final S3ConnectorId s3ConnectorId1 = new S3ConnectorId(id1);

    public final S3ConnectorId s3ConnectorId2 = new S3ConnectorId(id2);

    @Test
    public void testEquals() {
        assertTrue(s3ConnectorId.equals(s3ConnectorId2));
    }

    @Test
    public void testNotEqual() {
        assertFalse(s3ConnectorId.equals(s3ConnectorId1));
    }

    @Test
    public void testToString() {
        assertEquals(s3ConnectorId.toString(), "connectorId");
    }

    @Test
    public void testHashCode() {
        assertEquals(s3ConnectorId.hashCode(), Objects.hash(id));
    }
}
