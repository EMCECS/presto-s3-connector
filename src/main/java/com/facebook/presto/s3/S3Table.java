/*
 *  Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;
import static com.facebook.presto.s3.S3Const.*;

public class S3Table {
    private final String name;
    private final List<S3Column> columns;
    private final List<ColumnMetadata> columnsMetadata;
    private final String objectDataFormat;
    private final String hasHeaderRow;
    private final String recordDelimiter;
    private final String fieldDelimiter;
    private final String tableBucketName;
    private final String tableBucketPrefix;
    private Map<String, List<String>> sources;

    @JsonCreator
    public S3Table(
            @JsonProperty("name") String name,
            @JsonProperty("columns") List<S3Column> columns,
            @JsonProperty("objectDataFormat") String objectDataFormat,
            @JsonProperty("hasHeaderRow") String hasHeaderRow,
            @JsonProperty("recordDelimiter") String recordDelimiter,
            @JsonProperty("fieldDelimiter") String fieldDelimiter,
            @JsonProperty("tableBucketName") String tableBucketName,
            @JsonProperty("tableBucketPrefix") String tableBucketPrefix,
            @JsonProperty("sources") Map<String, List<String>> sources) {

        this.sources = requireNonNull(sources, "sources is null");
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = requireNonNull(name, "name is null");
        this.objectDataFormat = requireNonNull(objectDataFormat, "objectDataFormat is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.hasHeaderRow = hasHeaderRow != null ? hasHeaderRow : DEFAULT_HAS_HEADER_ROW;
        this.recordDelimiter = recordDelimiter != null ? recordDelimiter : DEFAULT_RECORD_DELIMITER;
        this.fieldDelimiter = fieldDelimiter != null ? fieldDelimiter : DEFAULT_FIELD_DELIMITER;
        if (tableBucketName == null) {
            // Typically, there is one bucket, and one prefix location for this table
            // Using JSON, it's possible to define multiple buckets and prefix locations
            // Using Presto CLI, you specify one external location 's3a://bucket/prefix
            // Be able to parse this: "testbucket":["cvdata/VehicleLog1.csv", "cvdata/VehicleLog2.csv" ]
            this.tableBucketName = (String) sources.keySet().toArray()[0];
            this.tableBucketPrefix = (String) sources.get(this.tableBucketName).toArray()[0];
        } else {
            this.tableBucketName = tableBucketName;
            this.tableBucketPrefix = tableBucketPrefix;
        }

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (S3Column column : this.columns) {
            columnsMetadata.add(new ColumnMetadata(column.getName(), column.getType()));
        }
        this.columnsMetadata = columnsMetadata.build();
    }

    public S3Table(String name) {
        this.name = name;
        columns = null;
        columnsMetadata = null;
        objectDataFormat = null;
        hasHeaderRow = null;
        recordDelimiter = null;
        fieldDelimiter = null;
        tableBucketPrefix = null;
        tableBucketName = null;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getObjectDataFormat() {
        return objectDataFormat;
    }

    @JsonProperty
    public List<S3Column> getColumns() {
        return columns;
    }

    public List<ColumnMetadata> getColumnsMetadata() {
        return columnsMetadata;
    }

    @JsonProperty
    public Map<String, List<String>> getSources() {
        return sources;
    }

    @JsonProperty
    public String getHasHeaderRow() {
        return hasHeaderRow;
    }

    @JsonProperty
    public String getRecordDelimiter() {
        return recordDelimiter;
    }

    @JsonProperty
    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    @JsonProperty
    public String getTableBucketName() {
        return tableBucketName;
    }

    @JsonProperty
    public String getTableBucketPrefix() {
        return tableBucketPrefix;
    }
}
