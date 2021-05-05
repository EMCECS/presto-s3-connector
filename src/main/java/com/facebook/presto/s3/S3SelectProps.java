/*
 * Copyright (c) Pravega Authors.
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

public class S3SelectProps
{
    // TODO: make configurable
    // https://github.com/pravega/pravega-sql/issues/75
    private String fieldDelim = ",";

    private String recordDelim = "\n";

    private final boolean useHeader;

    public S3SelectProps(boolean useHeader, String definedRecordDelimiter,
                         String definedFieldDelimiter)
    {
        this.useHeader = useHeader;
        if (definedRecordDelimiter != null) {
            recordDelim = definedRecordDelimiter;
        }

        if (definedFieldDelimiter != null) {
            fieldDelim = definedFieldDelimiter;
        }
    }

    public String getFieldDelim()
    {
        return fieldDelim;
    }

    public String getRecordDelim()
    {
        return recordDelim;
    }

    public boolean getUseHeader()
    {
        return useHeader;
    }
}
