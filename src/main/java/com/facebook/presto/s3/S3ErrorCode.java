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

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;
import static com.facebook.presto.spi.ErrorType.USER_ERROR;

public enum S3ErrorCode
        implements ErrorCodeSupplier {
    S3_INVALID_METADATA(0, USER_ERROR),
    S3_UNSUPPORTED_FORMAT(1, USER_ERROR),
    S3_TABLE_ALREADY_EXISTS(2, USER_ERROR),
    S3_SCHEMA_ALREADY_EXISTS(3, USER_ERROR),
    S3_FILESYSTEM_ERROR(4, EXTERNAL),
    S3_CURSOR_ERROR(5, EXTERNAL),
    S3_BAD_DATA(6, EXTERNAL),
    S3_CANNOT_OPEN_SPLIT(7, EXTERNAL),
    S3_MISSING_DATA(8, EXTERNAL),
    S3_PARTITION_SCHEMA_MISMATCH(9, EXTERNAL);

    private final ErrorCode errorCode;

    S3ErrorCode(int code, ErrorType type) {
        errorCode = new ErrorCode(code + 0x0508_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}
