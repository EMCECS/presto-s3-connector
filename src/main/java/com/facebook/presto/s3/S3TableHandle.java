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

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class S3TableHandle
        implements ConnectorTableHandle {
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final String objectDataFormat;
    private final String fieldDelimiter;
    private final String recordDelimiter;
    private final String hasHeaderRow;
    private final String tableBucketName;
    private final String tableBucketPrefix;
    private final Map<String, List<String>> bucketObjectsMap;

    @JsonCreator
    public S3TableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("objectDataFormat") String objectDataFormat,
            @JsonProperty("fieldDelimiter") String fieldDelimiter,
            @JsonProperty("recordDelimiter") String recordDelimiter,
            @JsonProperty("hasHeaderRow") String hasHeaderRow,
            @JsonProperty("tableBucketName") String tableBucketName,
            @JsonProperty("tableBucketPrefix") String tableBucketPrefix,
            @JsonProperty("bucketObjectsMap") Map<String, List<String>> bucketObjectsMap
    ) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.objectDataFormat = requireNonNull(objectDataFormat, "objectDataFormat is null");
        this.fieldDelimiter = requireNonNull(fieldDelimiter, "fieldDelimiter is null");
        this.recordDelimiter = requireNonNull(recordDelimiter, "recordDelimiter is null");
        this.hasHeaderRow = requireNonNull(hasHeaderRow, "hasHeaderRow is null");
        this.tableBucketName = requireNonNull(tableBucketName, "tableBucketName is null");
        this.tableBucketPrefix = tableBucketPrefix != null ? tableBucketPrefix : "/";
        this.bucketObjectsMap = requireNonNull(bucketObjectsMap, "bucketObjectsMap is null");
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
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public String getObjectDataFormat() {
        return objectDataFormat;
    }

    @JsonProperty
    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    @JsonProperty
    public String getRecordDelimiter() {
        return recordDelimiter;
    }

    @JsonProperty
    public String getHasHeaderRow() {
        return hasHeaderRow;
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
    public Map<String, List<String>> getBucketObjectsMap() {
        return bucketObjectsMap;
    }

    public SchemaTableName toSchemaTableName() {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorId, schemaName, tableName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        com.facebook.presto.s3.S3TableHandle other = (com.facebook.presto.s3.S3TableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName);
    }

    @Override
    public String toString() {
        return Joiner.on(":").join(connectorId, schemaName, tableName);
    }
}
