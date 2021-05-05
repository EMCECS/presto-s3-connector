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

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.common.type.Type;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class S3ColumnHandle
        implements DecoderColumnHandle, Comparable<S3ColumnHandle> {

    // when using s3 select we need to know both relative and absolute positions within a schema
    // relative is for the index into the data returned, and absolute so we know which column positions
    // to select from S3

    // may be relative to select projections - position within selected columns
    private final int ordinalPosition;
    private final String mapping;
    // this is position within the schema itself
    private final int absoluteSchemaPosition;
    /*
        for example:

        select * from table
                schema   ordinal
        vin        0        0
        timestamp  1        1
        subsystem  2        2
        message    3        3

        select timestamp, message from table
                schema   ordinal
        timestamp  1        0
        message    3        1
     */

    private final String connectorId;
    private final String name;
    private final Type type;
    private final String dataFormat;
    private final String formatHint;
    private final boolean keyDecoder;
    private final boolean hidden;
    private final boolean internal;

    @JsonCreator
    public S3ColumnHandle(
        @JsonProperty("connectorId") String connectorId,
        @JsonProperty("ordinalPosition") int ordinalPosition,
        @JsonProperty("name") String name,
        @JsonProperty("type") Type type,
        @JsonProperty("mapping") String mapping,
        @JsonProperty("dataFormat") String dataFormat,
        @JsonProperty("formatHint") String formatHint,
        @JsonProperty("keyDecoder") boolean keyDecoder,
        @JsonProperty("hidden") boolean hidden,
        @JsonProperty("internal") boolean internal){
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.ordinalPosition = ordinalPosition;
        this.name = requireNonNull(name, "columnName is null");
        this.type = requireNonNull(type, "columnType is null");
        this.mapping = mapping;
        this.dataFormat = dataFormat;
        this.formatHint = formatHint;
        this.keyDecoder = keyDecoder;
        this.hidden = hidden;
        this.internal = internal;

        this.absoluteSchemaPosition = ordinalPosition;
    }

    // copy ctor to update position+mapping.  use case is s3select which may not return all columns
    public S3ColumnHandle(S3ColumnHandle columnHandle, int position)
    {
        this.connectorId = columnHandle.connectorId;
        this.ordinalPosition = position;
        this.absoluteSchemaPosition = columnHandle.absoluteSchemaPosition;
        this.name = columnHandle.name;
        this.type = columnHandle.type;
        this.mapping = String.valueOf(position);
        this.dataFormat = columnHandle.dataFormat;
        this.formatHint = columnHandle.formatHint;
        this.keyDecoder = columnHandle.keyDecoder;
        this.hidden = columnHandle.hidden;
        this.internal = columnHandle.internal;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @JsonProperty
    public int getAbsoluteSchemaPosition()
    {
        return absoluteSchemaPosition;
    }

    @Override
    @JsonProperty
    public String getName()
    {
        return name;
    }

    @Override
    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @Override
    @JsonProperty
    public String getMapping()
    {
        return mapping;
    }

    @Override
    @JsonProperty
    public String getDataFormat()
    {
        return dataFormat;
    }

    @Override
    @JsonProperty
    public String getFormatHint()
    {
        return formatHint;
    }

    @JsonProperty
    public boolean isKeyDecoder() { return keyDecoder; }

    @JsonProperty
    public boolean isHidden()
    {
        return hidden;
    }

    @Override
    @JsonProperty
    public boolean isInternal()
    {
        return internal;
    }

    public ColumnMetadata getColumnMetadata() {
        return new ColumnMetadata(name, type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorId, name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        S3ColumnHandle other = (S3ColumnHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.ordinalPosition, other.ordinalPosition) &&
                Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.mapping, other.mapping) &&
                Objects.equals(this.dataFormat, other.dataFormat) &&
                Objects.equals(this.formatHint, other.formatHint) &&
                Objects.equals(this.keyDecoder, other.keyDecoder) &&
                Objects.equals(this.hidden, other.hidden) &&
                Objects.equals(this.internal, other.internal);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("name", name)
                .add("type", type)
                .add("ordinalPosition", ordinalPosition)
                .add("mapping", mapping)
                .add("dataFormat", dataFormat)
                .add("formatHind", formatHint)
                .add("keyDecoder", keyDecoder)
                .add("hidden", hidden)
                .add("internal", internal)
                .toString();
    }

    @Override
    public int compareTo(S3ColumnHandle s3ColumnHandle) {
        return Integer.compare(this.getOrdinalPosition(), this.getOrdinalPosition());
    }

}
