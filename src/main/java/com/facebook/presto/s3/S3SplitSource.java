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

import com.facebook.airlift.concurrent.MoreFutures;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class S3SplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(com.facebook.presto.s3.S3SplitSource.class);

    private final Iterator<S3ObjectRange> objects;
    private final int max;
    private final int throttleMs;
    private final String connectorId;
    private final S3ConnectorConfig config;
    private final S3TableLayoutHandle layoutHandle;
    private final Optional<String> objectDataSchemaContents;
    private final boolean s3SelectEnabled;

    private boolean first = true;

    private static ListeningScheduledExecutorService tp =
            MoreExecutors.listeningDecorator(new ScheduledThreadPoolExecutor(10));

    public S3SplitSource(String connectorId,
                         S3ConnectorConfig config,
                         S3TableLayoutHandle layoutHandle,
                         Optional<String> objectDataSchemaContents,
                         boolean s3SelectEnabled,
                         Iterator<S3ObjectRange> objects,
                         int max,
                         int throttleMs)
    {
        this.connectorId = connectorId;
        this.config = config;
        this.layoutHandle = layoutHandle;
        this.objectDataSchemaContents = objectDataSchemaContents;
        this.s3SelectEnabled = s3SelectEnabled;
        this.objects = objects;
        this.max = max;
        this.throttleMs= throttleMs;
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        // TODO: we can list buckets + figure out ranges here instead of in S3ObjectManager
        //       pass bucket name / param ando listing here

        // TODO: delaying the return of splits is for later testing large number of splits, and processing right away

        ListenableScheduledFuture<ConnectorSplitBatch> future = tp.schedule(() -> {
            List<ConnectorSplit> results = new ArrayList<>();
            int size = Math.min(max, maxSize);
            int i = 0;
            while (!isFinished() && i++ < size) {
                S3ObjectRange objectRange = objects.next();
                S3Split split = new S3Split(
                        config.getS3Port(),
                        config.getS3Nodes(),
                        connectorId,
                        layoutHandle,
                        objectDataSchemaContents,
                        s3SelectEnabled,
                        S3Util.serialize(objectRange));
                results.add(split);
            }
            log.info("return " + results.size() + " splits");
            return new ConnectorSplitBatch(results, isFinished());
        }, first ? 10 : throttleMs, TimeUnit.MILLISECONDS);
        first = false;
        return MoreFutures.toCompletableFuture(future);
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean isFinished()
    {
        return !objects.hasNext();
    }
}
