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

import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.connector.*;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.s3.S3Util.buildSessionPropertyList;
import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;
import static com.facebook.presto.s3.S3Const.*;

public class S3Connector
        implements Connector {
    private static final Logger log = Logger.get(com.facebook.presto.s3.S3Connector.class);

    private final LifeCycleManager lifeCycleManager;
    private final ConnectorMetadata metadata;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final ConnectorPageSinkProvider pageSinkProvider;

    @Inject
    public S3Connector(
            LifeCycleManager lifeCycleManager,
            ConnectorMetadata metadata,
            ConnectorSplitManager splitManager,
            ConnectorPageSourceProvider pageSourceProvider,
            ConnectorPageSinkProvider pageSinkProvider) {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return S3TransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }


    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return buildSessionPropertyList();
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties() {
        return ImmutableList.of(
                stringProperty(OBJECT_FILE_TYPE_DEF, "Format of table", DEFAULT_OBJECT_FILE_TYPE, false),
                stringProperty(EXTERNAL_LOCATION_DEF, "Location of table data", "", false),
                stringProperty(HAS_HEADER_ROW_DEF, "Whether or not the first row is a header", DEFAULT_HAS_HEADER_ROW, false),
                stringProperty(RECORD_DELIMITER_DEF, "Delimiter between rows", DEFAULT_RECORD_DELIMITER, false),
                stringProperty(FIELD_DELIMITER_DEF, "Delimiter between fields in a record", DEFAULT_FIELD_DELIMITER, false));
    }

    @Override
    public final void shutdown() {
        try {
            lifeCycleManager.stop();
        } catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
