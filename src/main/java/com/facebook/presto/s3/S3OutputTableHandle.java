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

import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class S3OutputTableHandle
        implements ConnectorOutputTableHandle
{
    private final String connectorId;
    private final SchemaTableName schemaTableName;
    private final Map<String, Object> properties;
    private final List<S3Column> columns;

    @JsonCreator
    public S3OutputTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("properties") Map<String, Object> properties,
            @JsonProperty("columns") List<S3Column> columns)
    {
        this.connectorId = requireNonNull(connectorId, "clientId is null");
        this.properties = requireNonNull(properties, "tableProperties is null");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        requireNonNull(columns, "columns is null");
        this.columns = ImmutableList.copyOf(columns);
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public Map<String, Object> getProperties() { return properties; }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public List<S3Column> getColumns()
    {
        return columns;
    }

    public List<String> getColumnNames() {
        ImmutableList.Builder<String> columNames = ImmutableList.builder();
        for (S3Column s3column : columns) {
            columNames.add(s3column.getName());
        }
        return columNames.build();
    }

    public List<Type> getColumnTypes() {
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        for (S3Column s3column : columns) {
            columnTypes.add(s3column.getType());
        }
        return columnTypes.build();
    }

    @Override
    public String toString()
    {
        return "s3:" + schemaTableName.getSchemaName() + "." + schemaTableName.getTableName();
    }




}
