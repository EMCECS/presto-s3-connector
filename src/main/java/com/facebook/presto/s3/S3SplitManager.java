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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.*;

import static com.facebook.presto.s3.S3Const.*;
import static com.facebook.presto.s3.S3Util.boolProp;
import static com.facebook.presto.s3.S3Util.intProp;
import static com.facebook.presto.s3.Types.checkType;

import static java.util.Objects.requireNonNull;

public class S3SplitManager
        implements ConnectorSplitManager {
    private final String connectorId;
    private final S3ConnectorConfig s3ConnectorConfig;
    private final S3AccessObject s3AccessObject;
    private static final Logger log = Logger.get(S3SplitManager.class);

    @Inject
    public S3SplitManager(S3ConnectorConfig s3ConnectorConfig,
                          S3ConnectorId connectorId,
                          S3AccessObject s3AccessObject) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.s3ConnectorConfig = requireNonNull(s3ConnectorConfig, "config is null");
        this.s3AccessObject = requireNonNull(s3AccessObject, "S3 S3 access object is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingContext splitSchedulingContext) {

        S3TableLayoutHandle layoutHandle = checkType(layout, S3TableLayoutHandle.class, "layout");

        Iterator<S3ObjectRange> objectContentsIterator;
        if(!layoutHandle.getTable().getSchemaName().equals("s3_buckets")) {
            // higher batch size here, so we can answer isFinished() without reading more from server
            int batchSize = intProp(session, SESSION_PROP_SPLIT_BATCH, 100);
            batchSize = batchSize + batchSize / 2;
            long rangeBytes = intProp(session, SESSION_PROP_SPLIT_RANGE_MB, 32) * 1024 * 1024;
            objectContentsIterator = new S3ObjectRangeIterator(s3AccessObject.getS3Client(),
                    layoutHandle.getTable().getBucketObjectsMap(),
                    rangeBytes, batchSize);
        } else {
                objectContentsIterator = new Iterator<S3ObjectRange>() {
                int objectCount = layoutHandle.getTable().getBucketObjectsMap().size();
                @Override
                public boolean hasNext() {
                    return objectCount-- > 0;
                }

                @Override
                public S3ObjectRange next() {
                    return new S3ObjectRange(null, null);
                }
            };
        }

        return new S3SplitSource(connectorId,
                s3ConnectorConfig,
                layoutHandle,
                boolProp(session, SESSION_PROP_S3_SELECT_PUSHDOWN, false),
                intProp(session, SESSION_PROP_SPLIT_BATCH, 100),
                intProp(session, SESSION_PROP_SPLIT_PAUSE_MS, 10),
                objectContentsIterator);
    }
}
