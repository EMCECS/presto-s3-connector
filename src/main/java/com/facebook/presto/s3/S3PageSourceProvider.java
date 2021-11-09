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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.decoder.DispatchingRowDecoderFactory;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import javax.inject.Inject;
import java.util.*;
import java.util.stream.Collectors;

import static com.facebook.presto.s3.S3Const.*;
import static com.facebook.presto.s3.S3HandleResolver.convertSplit;
import static com.facebook.presto.s3.S3SelectUtil.useS3Pushdown;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class S3PageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final TypeManager typeManager;
    private final DispatchingRowDecoderFactory decoderFactory;
    private final Set<S3BatchPageSourceFactory> pageSourceFactories;
    private final S3AccessObject accessObject;
    private static final Logger log = Logger.get(com.facebook.presto.s3.S3PageSourceProvider.class);

    @Inject
    public S3PageSourceProvider(TypeManager typeManager,
                                DispatchingRowDecoderFactory decoderFactory,
                                Set<S3BatchPageSourceFactory> pageSourceFactories,
                                S3AccessObject accessObject)
    {
        this.typeManager = typeManager;
        this.pageSourceFactories = ImmutableSet.copyOf(requireNonNull(pageSourceFactories, "pageSourceFactories is null"));
        this.decoderFactory = requireNonNull(decoderFactory, "decoderFactory is null");
        this.accessObject = requireNonNull(accessObject, "accessObject is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableLayoutHandle layout,
            List<ColumnHandle> columns,
            SplitContext splitContext)
    {

        S3Split s3Split = convertSplit(split);
        S3TableHandle s3TableHandle = s3Split.getS3TableHandle();
        if(!s3TableHandle.getObjectDataFormat().equalsIgnoreCase(PARQUET)) {
            return notParquetPageSourceHelper(columns, s3Split, s3TableHandle, session);
        } else {
           return parquetPageSourceHelper(columns, s3Split, s3TableHandle, session, splitContext);
        }

    }

    private ConnectorPageSource notParquetPageSourceHelper(List<ColumnHandle> columns, S3Split s3Split, S3TableHandle s3TableHandle, ConnectorSession session){
        List<S3ColumnHandle> s3Columns = columns.stream()
                .map(S3HandleResolver::convertColumnHandle)
                .collect(Collectors.toList());

        if (useS3Pushdown(s3Split, s3TableHandle.getObjectDataFormat())) {
            // update relative positions for s3 select so we can properly
            // get index into returned data
            for (int i = 0; i < s3Columns.size(); i++) {
                s3Columns.set(i, new S3ColumnHandle(s3Columns.get(i), i));
            }
        }

        RowDecoder objectDecoder = null;
        if (s3TableHandle.getObjectDataFormat().equalsIgnoreCase(JSON)) {
            // only json uses this
            objectDecoder = decoderFactory.create(
                    s3TableHandle.getObjectDataFormat(),
                    getDecoderParameters(s3Split.getObjectDataSchemaContents()),
                    s3Columns.stream()
                            .filter(col -> !col.isInternal())
                            .filter(S3ColumnHandle::isKeyDecoder)
                            .collect(toImmutableSet()));
        }

        return new RecordPageSource(new S3RecordSet(session, s3Split, s3Columns, accessObject, objectDecoder, s3TableHandle));
    }

    private ConnectorPageSource parquetPageSourceHelper(List<ColumnHandle> columns, S3Split s3Split, S3TableHandle s3TableHandle, ConnectorSession session, SplitContext splitContext){
        S3ObjectRange obj  = S3ObjectRange.deserialize(s3Split.getObjectRange());
        long start = obj.getOffset();
        int length  = obj.getLength();
        S3TableLayoutHandle s3Layout = s3Split.getS3TableLayoutHandle();
        String bucket = s3Layout.getTable().getBucketObjectsMap().keySet().iterator().next();
        String object = s3Layout.getTable().getBucketObjectsMap().get(bucket).get(0);
        List<S3ColumnHandle> selectedColumns = columns.stream()
                .map(S3ColumnHandle.class::cast)
                .collect(toList());
        TupleDomain<S3ColumnHandle> effectivePredicate = s3Layout.getConstraints()
                .transform(S3ColumnHandle.class::cast);

        Optional<ConnectorPageSource> pageSource = createS3PageSource(
                pageSourceFactories,
                session,
                bucket,
                object,
                start,
                length,
                selectedColumns,
                splitContext.getDynamicFilterPredicate().map(filter -> filter.transform(handle -> (S3ColumnHandle) handle).intersect(effectivePredicate)).orElse(effectivePredicate),
                typeManager,
                s3TableHandle.toSchemaTableName(),
                s3Split.getS3SelectPushdownEnabled());
        if (pageSource.isPresent()) {
            return pageSource.get();
        }
        throw new IllegalStateException("Could not find a file reader for split " + s3Split);
    }


    public static Optional<ConnectorPageSource> createS3PageSource(
            Set<S3BatchPageSourceFactory> pageSourceFactories,
            ConnectorSession session,
            String bucket,
            String object,
            long start,
            int length,
            List<S3ColumnHandle> s3Columns,
            TupleDomain<S3ColumnHandle> effectivePredicate,
            TypeManager typeManager,
            SchemaTableName tableName,
            boolean s3SelectPushdownEnabled)
    {
        List<S3ColumnHandle> allColumns = s3Columns;

        List<com.facebook.presto.s3.S3PageSourceProvider.ColumnMapping> columnMappings = com.facebook.presto.s3.S3PageSourceProvider.ColumnMapping.buildColumnMappings(
                allColumns);

        for (S3BatchPageSourceFactory pageSourceFactory : pageSourceFactories) {
            Optional<? extends ConnectorPageSource> pageSource = pageSourceFactory.createPageSource(
                    session,
                    bucket,
                    object,
                    start,
                    length,
                    tableName,
                    ColumnMapping.toColumnHandles(columnMappings),
                    effectivePredicate);
            if (pageSource.isPresent()) {
                S3PageSource s3PageSource = new S3PageSource(
                        columnMappings,
                        typeManager,
                        pageSource.get());
                return Optional.of(s3PageSource);
            }
        }


        return Optional.empty();
    }
    private Map<String, String> getDecoderParameters(Optional<String> dataSchema)
    {
        ImmutableMap.Builder<String, String> parameters = ImmutableMap.builder();
        dataSchema.ifPresent(schema -> parameters.put("dataSchema", schema));
        return parameters.build();
    }

    public static class ColumnMapping
    {
        private final S3ColumnHandle s3ColumnHandle;
        /**
         * ordinal of this column in the underlying page source or record cursor
         */
        private final OptionalInt index;


        private ColumnMapping(S3ColumnHandle s3ColumnHandle, OptionalInt index)
        {
            this.s3ColumnHandle = requireNonNull(s3ColumnHandle, "s3ColumnHandle is null");
            this.index = requireNonNull(index, "index is null");
        }
        public static com.facebook.presto.s3.S3PageSourceProvider.ColumnMapping regular(S3ColumnHandle s3ColumnHandle, int index)
        {
            return new com.facebook.presto.s3.S3PageSourceProvider.ColumnMapping(s3ColumnHandle, OptionalInt.of(index));
        }

        public S3ColumnHandle getS3ColumnHandle()
        {
            return s3ColumnHandle;
        }

        public int getIndex()
        {
            return index.getAsInt();
        }


        public static List<ColumnMapping> buildColumnMappings(
                List<S3ColumnHandle> columns)
        {
            int regularIndex = 0;
            Set<Integer> regularColumnIndices = new HashSet<>();
            ImmutableList.Builder<ColumnMapping> columnMappings = ImmutableList.builder();
            for (S3ColumnHandle column : columns) {

                checkArgument(regularColumnIndices.add(column.getOrdinalPosition()), "duplicate s3ColumnIndex in columns list");
                columnMappings.add(regular(column, regularIndex));
                regularIndex++;
            }
            return columnMappings.build();
        }

        public static List<S3ColumnHandle> toColumnHandles(List<ColumnMapping> regularColumnMappings)
        {
            return regularColumnMappings.stream()
                    .map(columnMapping -> {
                        S3ColumnHandle columnHandle = columnMapping.getS3ColumnHandle();
                        return new S3ColumnHandle(
                                columnHandle.getConnectorId(),
                                columnHandle.getOrdinalPosition(),
                                columnHandle.getName(),
                                columnHandle.getType(),
                                columnHandle.getMapping(),
                                columnHandle.getDataFormat(),
                                columnHandle.getFormatHint(),
                                columnHandle.isKeyDecoder(),
                                columnHandle.isHidden(),
                                columnHandle.isInternal());
                    })
                    .collect(toList());
        }
    }
}
