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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigSecuritySensitive;
import com.facebook.presto.spi.HostAddress;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import javax.validation.constraints.NotNull;
import java.util.List;

import static com.google.common.collect.Iterables.transform;

public class S3ConnectorConfig
{
    private String S3SchemaFileLocationDir;
    private int S3Port;
    private String S3UserKey;
    private String S3UserSecretKey;
    private List<HostAddress> S3Nodes;
    private boolean isSecureS3;
    private HostAddress SchemaRegistryServerIP;
    private int SchemaRegistryServerPort;
    private String SchemaRegistryServerNamespace;
    private int maxConnections = 100;
    private int s3ClientExecutionTimeout = 10000;
    private int s3SocketTimeout = 10000;
    private int s3ConnectionTimeout = 10000;

    @NotNull
    public String getS3SchemaFileLocationDir(){
        return S3SchemaFileLocationDir;
    }

    @NotNull
    public int getS3Port(){
        return S3Port;
    }

    @NotNull
    public int getMaxConnections(){ return maxConnections; }

    @NotNull
    public int getS3ClientExecutionTimeout() { return s3ClientExecutionTimeout; }

    @NotNull
    public int getS3SocketTimeout() { return s3SocketTimeout; }

    @NotNull
    public int getS3ConnectionTimeout() { return s3ConnectionTimeout; }

    @NotNull
    public String  getS3UserKey() {
        return S3UserKey;
    }

    @NotNull
    public String getS3UserSecretKey() {
        return S3UserSecretKey;
    }

    @NotNull
    public List<HostAddress> getS3Nodes () {
        return S3Nodes;
    }

    public HostAddress getRandomS3Node () {
        return S3Nodes.get((int)Math.random() * S3Nodes.size());
    }

    @NotNull
    public boolean getisSecureS3 () {
        return isSecureS3;
    }

    @NotNull
    public HostAddress getSchemaRegistryServerIP() { return SchemaRegistryServerIP; }

    @NotNull
    public int getSchemaRegistryServerPort() { return  SchemaRegistryServerPort; }

    @NotNull
    public String getSchemaRegistryServerNamespace() { return SchemaRegistryServerNamespace; }

    @Config("s3.s3SchemaFileLocationDir")
    public com.facebook.presto.s3.S3ConnectorConfig setS3SchemaFileLocationDir (String S3SchemaFileLocationDir) {
        this.S3SchemaFileLocationDir = S3SchemaFileLocationDir;
        return this;
    }

    @Config("s3.s3Port")
    public com.facebook.presto.s3.S3ConnectorConfig setS3Port (int S3Port) {
        this.S3Port = S3Port;
        isSecureS3 = S3Port == 9021;
        return this;
    }

    @Config("s3.s3UserKey")
    @ConfigSecuritySensitive
    public com.facebook.presto.s3.S3ConnectorConfig setS3UserKey (String S3UserKey) {
        this.S3UserKey = S3UserKey;
        return this;
    }

    @Config("s3.s3UserSecretKey")
    @ConfigSecuritySensitive
    public com.facebook.presto.s3.S3ConnectorConfig setS3UserSecretKey (String S3UserSecretKey) {
        this.S3UserSecretKey = S3UserSecretKey;
        return this;
    }

    @Config("s3.s3Nodes")
    public com.facebook.presto.s3.S3ConnectorConfig setS3Nodes(String nodes) {
       this.S3Nodes = (nodes == null) ? null : parseNodes(nodes).asList();
       return this;
    }

    @Config("s3.schemaRegistryServerIP")
    public com.facebook.presto.s3.S3ConnectorConfig setSchemaRegistryServerIP(String SchemaRegistryServerIP) {
        this.SchemaRegistryServerIP = HostAddress.fromString(SchemaRegistryServerIP);
        return this;
    }

    @Config("s3.schemaRegistryPort")
    public com.facebook.presto.s3.S3ConnectorConfig setSchemaRegistryServerPort(int SchemaRegistryServerPort) {
        this.SchemaRegistryServerPort = SchemaRegistryServerPort;
        return this;
    }

    // TODO: https://github.com/pravega/pravega-sql/issues/82
    @Config("s3.schemaRegistryNamespace")
    public com.facebook.presto.s3.S3ConnectorConfig setSchemaRegistryServerNamespace(String SchemaRegistryServerNamespace) {
        this.SchemaRegistryServerNamespace = SchemaRegistryServerNamespace;
        return this;
    }

    @Config("s3.maxConnections")
    public com.facebook.presto.s3.S3ConnectorConfig setMaxConnections (int maxConnections) {
        this.maxConnections = maxConnections;
        return this;
    }

    @Config("s3.s3SocketTimeout")
    public com.facebook.presto.s3.S3ConnectorConfig setS3SocketTimeout (int s3SocketTimeout) {
        this.s3SocketTimeout = s3SocketTimeout;
        return this;
    }

    @Config("s3.s3ConnectionTimeout")
    public com.facebook.presto.s3.S3ConnectorConfig setS3ConnectionTimeout (int s3ConnectionTimeout) {
        this.s3ConnectionTimeout = s3ConnectionTimeout;
        return this;
    }

    @Config("s3.s3ClientExecutionTimeout")
    public com.facebook.presto.s3.S3ConnectorConfig setS3ClientExecutionTimeout (int s3ClientExecutionTimeout) {
        this.s3ClientExecutionTimeout = s3ClientExecutionTimeout;
        return this;
    }

    private static ImmutableSet<HostAddress> parseNodes(String S3Nodes)
    {
        Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
        return ImmutableSet.copyOf(transform(splitter.split(S3Nodes), com.facebook.presto.s3.S3ConnectorConfig::toHostAddress));
    }

    private static HostAddress toHostAddress(String value)
    {
        return HostAddress.fromString(value);
    }

}
