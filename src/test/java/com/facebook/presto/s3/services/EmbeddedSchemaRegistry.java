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
 *
 * Note: this class is based on SchemaRegistryUtils from pravega/flink-connectors
 * (rev 9332ad67e520c03c7122de1d3b90c6cafbf97634)
 * https://github.com/pravega/flink-connectors/blob/v0.9.0/src/test/java/io/pravega/connectors/flink/utils/SchemaRegistryUtils.java
 */
package com.facebook.presto.s3.services;

import java.io.Closeable;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.server.rest.RestServer;
import io.pravega.schemaregistry.server.rest.ServiceConfig;
import io.pravega.schemaregistry.service.SchemaRegistryService;
import io.pravega.schemaregistry.storage.SchemaStore;
import io.pravega.schemaregistry.storage.SchemaStoreFactory;

public class EmbeddedSchemaRegistry
        implements Closeable {
    private final static AtomicInteger servers = new AtomicInteger();

    private final int port;

    private final AtomicBoolean started = new AtomicBoolean();

    private final ScheduledExecutorService executor;

    private RestServer restServer;

    private SchemaRegistryClient client;

    public EmbeddedSchemaRegistry() {
        port = 9100 + servers.getAndIncrement();
        executor = Executors.newScheduledThreadPool(10);

        SchemaStore schemaStore = SchemaStoreFactory.createInMemoryStore(executor);

        restServer = new RestServer(
                new SchemaRegistryService(schemaStore, executor),
                ServiceConfig.builder().port(port).build()
        );
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            restServer.startAsync();
            restServer.awaitRunning();
        }
    }

    public void stop() {
        if (started.compareAndSet(true, false)) {
            restServer.stopAsync();
            restServer.awaitTerminated();
            executor.shutdownNow();
        }
    }

    public int port() {
        return port;
    }

    public URI getURI() {
        return URI.create("http://localhost:" + port);
    }

    public SchemaRegistryClient client() {
        if (client == null) {
            SchemaRegistryClientConfig config = SchemaRegistryClientConfig.builder()
                                                                          .schemaRegistryUri(getURI())
                                                                          .build();
            client = SchemaRegistryClientFactory.withDefaultNamespace(config);
        }
        return client;
    }

    @Override
    public void close() {
        try {
            stop();
        } catch (Exception quiet) {
        }
    }
}
