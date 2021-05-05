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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static com.facebook.presto.s3.S3Const.*;
import static com.facebook.presto.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;


public class S3PageSinkProvider
        implements ConnectorPageSinkProvider
{

    private final S3AccessObject accessObject;
    private static final Logger log = Logger.get(com.facebook.presto.s3.S3PageSinkProvider.class);

    @Inject
    public S3PageSinkProvider(S3AccessObject accessObject)
    {
        this.accessObject = accessObject;
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle, PageSinkContext pageSinkProperties)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof S3OutputTableHandle, "tableHandle is not an instance of S3OutputTableHandle");
        S3OutputTableHandle handle = (S3OutputTableHandle) tableHandle;
        String bucket = null;
        String prefix = null;
        String file_format = DEFAULT_OBJECT_FILE_TYPE;
        String has_header_row = DEFAULT_HAS_HEADER_ROW;
        String record_delimiter = DEFAULT_RECORD_DELIMITER;
        String field_delimiter = DEFAULT_FIELD_DELIMITER;

        for (Map.Entry<String, Object> property : handle.getProperties().entrySet()) {
            if (property.getKey().equalsIgnoreCase(EXTERNAL_LOCATION_DEF)) {
                String location = (String)property.getValue();
                try {
                    prefix = new URI(location).getPath();
                    if (prefix.startsWith("/")) {
                        prefix = prefix.replaceFirst("/", "");
                    }
                    bucket = new URI(location).getHost();
                } catch (URISyntaxException e) {
                    log.error("Incorrect location format: " + location);
                    throw new PrestoException(CONFIGURATION_INVALID,
                            format("Error processing schema string: %s", location));
                }
                log.debug("Table location. Bucket: " + bucket + ", prefix: " + prefix);
            } else if (property.getKey().equalsIgnoreCase(OBJECT_FILE_TYPE_DEF)) {
                file_format = (String)property.getValue();
                if (!S3Const.isValidFormatForInsert(file_format)){
                    throw new PrestoException(S3ErrorCode.S3_UNSUPPORTED_FORMAT,
                            format("Unsupported table format for insert: %s", (String)property.getValue()));
                }
            } else if (property.getKey().equalsIgnoreCase(HAS_HEADER_ROW_DEF)) {
                has_header_row = (String)property.getValue();
                log.debug("OutputTableHandle property. HasHeaderRow: " + has_header_row);
            } else if (property.getKey().equalsIgnoreCase(FIELD_DELIMITER_DEF)) {
                field_delimiter = (String)property.getValue();
                log.debug("OutputTableHandle property. FieldDelimiter: " + field_delimiter);
            } else if (property.getKey().equalsIgnoreCase(RECORD_DELIMITER_DEF)) {
                record_delimiter = (String)property.getValue();
                log.debug("OutputTableHandle property. RecordDelimiter: " + record_delimiter);

            }

        }

        return new S3PageSink(
                handle.getSchemaTableName().getSchemaName(),
                handle.getSchemaTableName().getTableName(),
                handle.getColumnNames(),
                handle.getColumnTypes(),
                bucket,
                prefix,
                file_format,
                has_header_row,
                record_delimiter,
                field_delimiter,
                false,
                accessObject);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle, PageSinkContext pageSinkProperties)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof S3InsertTableHandle, "tableHandle is not an instance of ConnectorInsertTableHandle");
        S3InsertTableHandle handle = (S3InsertTableHandle) tableHandle;
        String file_format = handle.getObjectDataFormat() != null ? handle.getObjectDataFormat() : DEFAULT_OBJECT_FILE_TYPE;
        String has_header_row = handle.getHasHeaderRow() != null ? handle.getHasHeaderRow() : DEFAULT_HAS_HEADER_ROW;
        String record_delimiter = handle.getRecordDelimiter() != null ? handle.getRecordDelimiter() : DEFAULT_RECORD_DELIMITER;
        String field_delimiter = handle.getFieldDelimiter() != null ? handle.getFieldDelimiter() : DEFAULT_FIELD_DELIMITER;
        return new S3PageSink(
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getColumnNames(),
                handle.getColumnTypes(),
                handle.getTableBucketName(),
                handle.getTableBucketPrefix(),
                file_format,
                has_header_row,
                record_delimiter,
                field_delimiter,
                false,
                accessObject);
    }


}
