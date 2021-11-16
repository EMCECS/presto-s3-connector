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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;

public class S3Split
        implements ConnectorSplit {
    private final String connectorId;
    private final boolean remotelyAccessible;
    private final List<HostAddress> s3Nodes;
    private final int S3Port;
    private final Optional<String> objectDataSchemaContents;
    private final boolean s3SelectPushdownEnabled;
    private final byte[] objectRange;
    private final S3TableLayoutHandle s3TableLayoutHandle;

    @JsonCreator
    public S3Split(
            @JsonProperty("S3Port") int S3Port,
            @JsonProperty("s3Nodes") List<HostAddress> s3Nodes,
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("s3TableLayoutHandle") S3TableLayoutHandle s3TableLayoutHandle,
            @JsonProperty("objectDataSchemaContents") Optional<String> objectDataSchemaContents,
            @JsonProperty("s3SelectPushdownEnabled") boolean s3SelectPushdownEnabled,
            @JsonProperty("objectRange") byte[] objectRange
    ) {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.s3Nodes = s3Nodes;
        this.S3Port = S3Port;
        this.remotelyAccessible = true;
        this.objectDataSchemaContents = objectDataSchemaContents;
        this.s3SelectPushdownEnabled = s3SelectPushdownEnabled;
        this.objectRange = requireNonNull(objectRange, "object bytes are null");
        this.s3TableLayoutHandle = requireNonNull(s3TableLayoutHandle, "s3TableLayoutHandle is null");
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public Optional<String> getObjectDataSchemaContents() {
        return objectDataSchemaContents;
    }

    @JsonProperty
    public byte[] getObjectRange() {
        return objectRange;
    }

    @JsonProperty
    public boolean getS3SelectPushdownEnabled() {
        return s3SelectPushdownEnabled;
    }

    @JsonProperty
    public S3TableHandle getS3TableHandle() {
        return s3TableLayoutHandle.getTable();
    }

    @JsonProperty
    public S3TableLayoutHandle getS3TableLayoutHandle() {
        return s3TableLayoutHandle;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy() {
        return NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates) {
        return s3Nodes;
    }

    @Override
    public Object getInfo() {
        return this;
    }
}
