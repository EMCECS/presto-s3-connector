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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static com.google.common.base.Preconditions.checkArgument;

public class S3InsertTableHandle
        implements ConnectorInsertTableHandle {
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final String tableBucketName;
    private final String tableBucketPrefix;
    private final String objectDataFormat;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final String HasHeaderRow;
    private final String RecordDelimiter;
    private final String FieldDelimiter;

    @JsonCreator
    public S3InsertTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("tableBucketName") String tableBucketName,
            @JsonProperty("tableBucketPrefix") String tableBucketPrefix,
            @JsonProperty("hasHeaderRow") String HasHeaderRow,
            @JsonProperty("recordDelimiter") String RecordDelimiter,
            @JsonProperty("fieldDelimiter") String FieldDelimiter,
            @JsonProperty("objectDataFormat") String objectDataFormat) {
        this.connectorId = requireNonNull(connectorId, "clientId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableBucketName = tableBucketName;
        this.tableBucketPrefix = tableBucketPrefix;
        this.objectDataFormat = objectDataFormat;
        this.HasHeaderRow = HasHeaderRow;
        this.RecordDelimiter = RecordDelimiter;
        this.FieldDelimiter = FieldDelimiter;
        requireNonNull(columnNames, "columnNames is null");
        requireNonNull(columnTypes, "columnTypes is null");
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes sizes don't match");
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.columnTypes = ImmutableList.copyOf(columnTypes);
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableBucketName() {
        return tableBucketName;
    }

    @JsonProperty
    public String getTableBucketPrefix() {
        return tableBucketPrefix;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public List<String> getColumnNames() {
        return columnNames;
    }

    @JsonProperty
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @JsonProperty
    public String getObjectDataFormat() {
        return objectDataFormat;
    }

    @JsonProperty
    public String getHasHeaderRow() {
        return HasHeaderRow;
    }

    @JsonProperty
    public String getRecordDelimiter() {
        return RecordDelimiter;
    }

    @JsonProperty
    public String getFieldDelimiter() {
        return FieldDelimiter;
    }

    @Override
    public String toString() {
        return "s3:" + schemaName + "." + tableName;
    }
}
