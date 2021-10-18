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
import com.facebook.presto.spi.FixedSplitSource;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import org.apache.avro.file.DataFileStream;

import javax.inject.Inject;

import java.io.InputStream;
import java.util.*;

import static com.facebook.presto.s3.S3Const.*;
import static com.facebook.presto.s3.S3Util.intProp;
import static com.facebook.presto.s3.Types.checkType;

import static java.util.Objects.requireNonNull;

public class S3SplitManager
        implements ConnectorSplitManager {
    private final String connectorId;
    private final S3ConnectorConfig s3ConnectorConfig;
    private final S3ObjectManager s3ObjectManager;
    private final S3AccessObject s3AccessObject;
    private static final Logger log = Logger.get(S3ConnectorFactory.class);

    @Inject
    public S3SplitManager(S3ConnectorConfig s3ConnectorConfig,
                          S3ConnectorId connectorId,
                          S3ObjectManager s3ObjectManager,
                          S3AccessObject s3AccessObject) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.s3ConnectorConfig = requireNonNull(s3ConnectorConfig, "config is null");
        this.s3ObjectManager = requireNonNull(s3ObjectManager, "objectManager is null");
        this.s3AccessObject = requireNonNull(s3AccessObject, "S3 S3 access object is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingContext splitSchedulingContext) {

        S3TableLayoutHandle layoutHandle = checkType(layout, S3TableLayoutHandle.class, "layout");

        Iterator<S3ObjectRange> objectContentsIterator;
        if(!layoutHandle.getTable().getSchemaName().equals("s3_buckets")) {
           objectContentsIterator = s3ObjectManager.getS3ObjectIterator(layoutHandle.getTable().getBucketObjectsMap(),
                   intProp(session, SESSION_PROP_SPLIT_RANGE_MB, 32));
        } else {
                objectContentsIterator = new Iterator<S3ObjectRange>() {
                int objectCount = layoutHandle.getTable().getBucketObjectsMap().size();
                @Override
                public boolean hasNext() {
                    return objectCount-- > 0;
                }

                @Override
                public S3ObjectRange next() {
                    return new S3ObjectRange(null, null, 0, Integer.MAX_VALUE);
                }
            };
        }
        List<ConnectorSplit> splits = new ArrayList<>();
        // Define objectDataSchemaContents if known (AVRO, ORC, etc)
        Optional<String> objectDataSchemaContents = Optional.empty();
        if (layoutHandle.getTable().getObjectDataFormat().equalsIgnoreCase(AVRO)) {
            String bucket = layoutHandle.getTable().getBucketObjectsMap().keySet().iterator().next();
            String object = layoutHandle.getTable().getBucketObjectsMap().get(bucket).get(0);
            log.debug("Creating splits for avro object \"" + object + "\" in bucket \"" + bucket + "\"");
            InputStream objectStream = s3AccessObject.getObject(bucket, object);
            try {
                final DataFileStream<GenericRecord> dataStream =
                        new DataFileStream<>(objectStream, new GenericDatumReader<>());
                objectDataSchemaContents = Optional.of(dataStream.getSchema().toString());
                log.debug("Read schema: " + objectDataSchemaContents.toString());
            } catch (Exception e) {
                log.error("Got an exception reading AVRO file: " + e);
                return null;
            }
        }


        while(objectContentsIterator.hasNext()) {
            S3Split split = new S3Split(
                    s3ConnectorConfig.getS3Port(),
                    s3ConnectorConfig.getS3Nodes(),
                    connectorId,
                    layoutHandle,
                    objectDataSchemaContents,
                    s3SelectEnabled(session),
                    S3ObjectRange.serialize(objectContentsIterator.next()));
            splits.add(split);
        }
        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }

    private boolean s3SelectEnabled(ConnectorSession session)
    {
        try {
            return session.getProperty(SESSION_PROP_S3_SELECT_PUSHDOWN, Boolean.class);
        }
        catch (PrestoException e) {
            return false;
        }
    }
}
