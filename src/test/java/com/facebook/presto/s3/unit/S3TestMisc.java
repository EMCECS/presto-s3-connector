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

import org.testng.annotations.Test;

import com.facebook.presto.s3.*;

import static org.testng.Assert.*;
import static com.facebook.presto.spi.ErrorType.USER_ERROR;


public class S3TestMisc {

    @Test
    public void testS3Const()
    {
        assertTrue(S3Const.isValidFormatForInsert("CsV"));
        assertFalse(S3Const.isValidFormatForInsert(S3Const.AVRO));
        assertFalse(S3Const.isValidFormatForQuery("Bogus"));
        assertTrue(S3Const.isValidFormatForQuery("AVRO"));
        assertTrue(S3Const.isValidFormatForQuery("parquet"));
    }

    @Test
    public void testS3ErrorCode() {
        S3ErrorCode code1 = S3ErrorCode.S3_INVALID_METADATA;
        assertEquals(code1.toErrorCode(), S3ErrorCode.S3_INVALID_METADATA.toErrorCode());
    }

}
