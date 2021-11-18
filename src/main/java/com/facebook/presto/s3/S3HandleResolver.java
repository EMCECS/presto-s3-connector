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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class S3HandleResolver
        implements ConnectorHandleResolver {
    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
        return S3TableLayoutHandle.class;
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass() {
        return S3TableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass() {
        return S3ColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass() {
        return S3Split.class;
    }

    @Override
    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass() {
        return S3TransactionHandle.class;
    }

    @Override
    public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass() {
        return S3InsertTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass() {
        return S3OutputTableHandle.class;
    }

    static S3Split convertSplit(ConnectorSplit split) {
        requireNonNull(split, "split is null");
        checkArgument(split instanceof S3Split, "split is not an instance of S3Split");
        return (S3Split) split;
    }

    static S3ColumnHandle convertColumnHandle(ColumnHandle columnHandle) {
        requireNonNull(columnHandle, "columnHandle is null");
        checkArgument(columnHandle instanceof S3ColumnHandle, "columnHandle is not an instance of S3ColumnHandle");
        return (S3ColumnHandle) columnHandle;
    }
}
