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

import static com.facebook.presto.s3.S3Const.CSV;
import static com.facebook.presto.s3.S3Const.TEXT;

public class S3SelectUtil {
    private S3SelectUtil() {
    }

    public static boolean useS3Pushdown(S3Split split, String objectDataFormat) {
        return split.getS3SelectPushdownEnabled() &&
                (objectDataFormat.equals(CSV) || objectDataFormat.equals(TEXT));
    }
}
