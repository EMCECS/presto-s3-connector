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

import com.facebook.presto.common.predicate.TupleDomain;
import com.google.common.collect.ImmutableList;
import com.facebook.presto.spi.HostAddress;
import org.testng.annotations.Test;

import com.facebook.presto.s3.*;

import java.util.List;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class S3SplitTest {

    private final int s3port = 9020;
    private final List<HostAddress> s3Nodes = ImmutableList.of(HostAddress.fromString("10.247.179.58:9020"));
    private final S3TableHandle s3TableHandle = new S3HandleTests().escTableHandle;
    private final Optional<String> objectDataSchemaContents = Optional.of("schema contents");
    private final String bucket = "bucket1";
    private final String key = "key1";
    private final long offset = 10;
    private final int length = 100;

    public final S3Split s3Split = new S3Split(
            s3port,
            s3Nodes,
            s3TableHandle.getConnectorId(),
            new S3TableLayoutHandle(s3TableHandle, TupleDomain.all()),
            objectDataSchemaContents,
            false /* s3SelectPushdownEnabled */,
            S3ObjectRange.serialize(new S3ObjectRange(bucket, key, offset, length, false))
    );

    @Test
    public void testGetConnectorId() {
        assertEquals(s3Split.getConnectorId(), "connectorId");
    }

    @Test
    public void testGetObjectDataSchemaContents() {
        assertEquals(s3Split.getObjectDataSchemaContents(), Optional.of("schema contents"));
    }

    @Test
    public void testGetObjectRange() {
        S3ObjectRange objectRange = S3ObjectRange.deserialize(s3Split.getObjectRange());
        assertEquals(bucket, objectRange.getBucket());
        assertEquals(key, objectRange.getKey());
        assertEquals(offset, objectRange.getOffset());
        assertEquals(length, objectRange.getLength());
    }

    @Test
    public void testGetS3TableHandle() {
        assertEquals(s3Split.getS3TableHandle(), new S3HandleTests().escTableHandle);
    }
}
