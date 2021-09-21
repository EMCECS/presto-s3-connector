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

import com.facebook.presto.spi.ConnectorSession;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class S3Const
{
    public static final String CSV = "csv";
    public static final String TEXT = "text";
    public static final String AVRO = "avro";
    public static final String JSON = "json";
    public static final String PARQUET = "parquet";
    public static final String LC_TRUE = "true";

    public static final String OBJECT_FILE_TYPE_DEF = "format";
    public static final String DEFAULT_OBJECT_FILE_TYPE = CSV;
    public static final String HAS_HEADER_ROW_DEF = "has_header_row";
    public static final String DEFAULT_HAS_HEADER_ROW = "false";
    public static final String FIELD_DELIMITER_DEF = "field_delimiter";
    public static final String DEFAULT_FIELD_DELIMITER = ",";
    public static final String RECORD_DELIMITER_DEF = "record_delimiter";
    public static final String DEFAULT_RECORD_DELIMITER = "\n";
    public static final String EXTERNAL_LOCATION_DEF = "external_location";

    public static final String SESSION_PROP_S3_SELECT_PUSHDOWN = "s3_select_pushdown_enabled";

    public static final String SESSION_PROP_SPLIT_BATCH = "split_batch";

    public static final String SESSION_PROP_SPLIT_PAUSE_MS = "split_pause_ms";

    public static final String SESSION_PROP_READER_BUFFER_SIZE_BYTES = "reader_buffer_size_bytes";

    public static final String SESSION_PROP_NEW_RECORD_CURSOR = "new_record_cursor";

    public static final String PARQUET_USE_COLUMN_NAME = "parquet_use_column_names";

    public static final String PARQUET_FAIL_WITH_CORRUPTED_STATISTICS = "parquet_fail_with_corrupted_statistics";

    public static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";

    public static final String PARQUET_BATCH_READ_OPTIMIZATION_ENABLED = "parquet_batch_read_optimization_enabled";

    public static final String PARQUET_BATCH_READER_VERIFICATION_ENABLED = "parquet_batch_reader_verification_enabled";

    public static final String SESSION_PROP_SPLIT_RANGE_MB = "split_range_mb";

    public static final String NO_TABLES = "$no-tables";

    public static final String TRUE = "true";
    public static final String FALSE = "false";


    public static final String JSON_PROP_TYPE = "type";
    public static final String JSON_PROP_FORMAT = "format";
    public static final String JSON_PROP_DATA_FORMAT = "dataFormat";
    public static final String JSON_PROP_NAME = "name";

    public static final String JSON_TYPE_DATE = "DATE";
    public static final String JSON_TYPE_VARCHAR = "VARCHAR";
    public static final String JSON_TYPE_BIGINT = "BIGINT";
    public static final String JSON_TYPE_DOUBLE = "DOUBLE";
    public static final String JSON_TYPE_BOOLEAN = "BOOLEAN";
    public static final String JSON_TYPE_INTEGER = "integer";
    public static final String JSON_TYPE_NUMBER = "number";
    public static final String JSON_TYPE_STRING = "string";
    public static final String JSON_VALUE_DATE_ISO = "iso8601";

    public static final String FORMAT_VALUE_DATE_TIME = "date-time";
    public static final String FORMAT_VALUE_DATE = "date";
    public static final String FORMAT_VALUE_TIME = "time";


    private static final List<String> validFormatsQuery = new ArrayList<>(
            Arrays.asList(CSV, TEXT, AVRO, JSON, PARQUET)
    );
    private static final List<String> validFormatsInsert = new ArrayList<>(
            Arrays.asList(CSV, JSON)
    );

    public S3Const()
    {
    }

    public static boolean isValidFormatForQuery(String format) {
        for (String valid : validFormatsQuery) {
            if (valid.equalsIgnoreCase(format)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isValidFormatForInsert(String format) {
        for (String valid : validFormatsInsert) {
            if (valid.equalsIgnoreCase(format)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isParquetBatchReadsEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_BATCH_READ_OPTIMIZATION_ENABLED, Boolean.class);
    }

    public static boolean isUseParquetColumnNames(ConnectorSession session)
    {
        return session.getProperty(PARQUET_USE_COLUMN_NAME, Boolean.class);
    }

    public static boolean isParquetBatchReaderVerificationEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_BATCH_READER_VERIFICATION_ENABLED, Boolean.class);
    }

    public static boolean isFailOnCorruptedParquetStatistics(ConnectorSession session)
    {
        return session.getProperty(PARQUET_FAIL_WITH_CORRUPTED_STATISTICS, Boolean.class);
    }

    public static DataSize getParquetMaxReadBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_MAX_READ_BLOCK_SIZE, DataSize.class);
    }
}
