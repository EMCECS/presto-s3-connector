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
 *
 * Note: This file contains changes from PrestoDb. Specifically the class is inspired from the
 * Hive Connector.
 * https://github.com/prestodb/presto/blob/master/presto-hive/src/main/java/com/facebook/presto/hive/parquet/ParquetPageSourceFactory.java
 */

package com.facebook.presto.s3.parquet;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.s3.S3ColumnHandle;
import com.facebook.presto.s3.S3AccessObject;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.ParquetCorruptionException;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.cache.ParquetMetadataSource;
import com.facebook.presto.parquet.predicate.Predicate;
import com.facebook.presto.parquet.reader.ParquetReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.security.AccessControlException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.StandardTypes.*;
import static com.facebook.presto.s3.S3Const.*;
import static com.facebook.presto.s3.S3ErrorCode.*;
import static com.facebook.presto.s3.parquet.S3ParquetDataSource.buildS3ParquetDataSource;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.parquet.ParquetTypeUtils.*;
import static com.facebook.presto.parquet.predicate.PredicateUtils.buildPredicate;
import static com.facebook.presto.parquet.predicate.PredicateUtils.predicateMatches;
import static com.facebook.presto.spi.StandardErrorCode.PERMISSION_DENIED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.io.ColumnIOConverter.constructField;

public class ParquetPageSourceFactory implements com.facebook.presto.s3.S3BatchPageSourceFactory {
    private final ParquetMetadataSource parquetMetadataSource;
    private final S3AccessObject accessObject;

    @Inject
    public ParquetPageSourceFactory(ParquetMetadataSource parquetMetadataSource,
                                    S3AccessObject accessObject)
    {
        this.parquetMetadataSource = requireNonNull(parquetMetadataSource, "parquetMetadataSource is null");
        this.accessObject = requireNonNull(accessObject, "hdfsEnvironment is null");
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            ConnectorSession session,
            String bucket,
            String key,
            long start,
            int length,
            SchemaTableName tableName,
            List<S3ColumnHandle> columns,
            TupleDomain<S3ColumnHandle> effectivePredicate) {

        return Optional.of(createParquetPageSource(
                accessObject,
                bucket,
                key,
                start,
                length,
                columns,
                tableName,
                isUseParquetColumnNames(session),
                isFailOnCorruptedParquetStatistics(session),
                getParquetMaxReadBlockSize(session),
                isParquetBatchReadsEnabled(session),
                isParquetBatchReaderVerificationEnabled(session),
                parquetMetadataSource,
                effectivePredicate));
    }

    public static ConnectorPageSource createParquetPageSource(
            S3AccessObject accessObject,
            String bucket,
            String key,
            long start,
            int length,
            List<S3ColumnHandle> columns,
            SchemaTableName tableName,
            boolean useParquetColumnNames,
            boolean failOnCorruptedParquetStatistics,
            DataSize maxReadBlockSize,
            boolean batchReaderEnabled,
            boolean verificationEnabled,
            ParquetMetadataSource parquetMetadataSource,
            TupleDomain<S3ColumnHandle> effectivePredicate)
    {
        AggregatedMemoryContext systemMemoryContext = newSimpleAggregatedMemoryContext();

        ParquetDataSource dataSource = null;
        try {
            FSDataInputStream inputStream = accessObject.getParquetObject(bucket, key, 65536);
            dataSource = buildS3ParquetDataSource(inputStream, bucket+key);
            long filesize  = accessObject.getObjectLength(bucket, key);
            ParquetMetadata parquetMetadata = parquetMetadataSource.getParquetMetadata(inputStream, dataSource.getId(), filesize, false).getParquetMetadata();


            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            Optional<MessageType> message = columns.stream()
                    .map(column -> getColumnType(column.getType(), fileSchema, useParquetColumnNames, column, tableName, bucket+key))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(type -> new MessageType(fileSchema.getName(), type))
                    .reduce(MessageType::union);

            MessageType requestedSchema = message.orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));

            ImmutableList.Builder<BlockMetaData> footerBlocks = ImmutableList.builder();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if (firstDataPage >= start && firstDataPage < start + length) {
                    footerBlocks.add(block);
                }
            }

            Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(descriptorsByPath, effectivePredicate);
            Predicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath);
            final ParquetDataSource finalDataSource = dataSource;
            ImmutableList.Builder<BlockMetaData> blocks = ImmutableList.builder();
            for (BlockMetaData block : footerBlocks.build()) {
                if (predicateMatches(parquetPredicate, block, finalDataSource, descriptorsByPath, parquetTupleDomain, failOnCorruptedParquetStatistics)) {
                    blocks.add(block);
                }
            }
            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
            ParquetReader parquetReader = new ParquetReader(
                    messageColumnIO,
                    blocks.build(),
                    dataSource,
                    systemMemoryContext,
                    maxReadBlockSize,
                    batchReaderEnabled,
                    verificationEnabled);

            ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
            for (S3ColumnHandle column : columns) {

                String name = column.getName();
                Type type = column.getType();

                namesBuilder.add(name);
                typesBuilder.add(type);

                if (getParquetType(type, fileSchema, useParquetColumnNames, column, tableName, bucket+key).isPresent()) {
                    String columnName = useParquetColumnNames ? name : fileSchema.getFields().get(column.getOrdinalPosition()).getName();
                    fieldsBuilder.add(constructField(type, lookupColumnByName(messageColumnIO, columnName)));
                }
                else {
                    fieldsBuilder.add(Optional.empty());
                }
            }
            return new ParquetPageSource(parquetReader, typesBuilder.build(), fieldsBuilder.build());
        }
        catch (Exception e) {
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            }
            catch (IOException ignored) {
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            if (e instanceof ParquetCorruptionException) {
                throw new PrestoException(S3_BAD_DATA, e);
            }
            if (e instanceof AccessControlException) {
                throw new PrestoException(PERMISSION_DENIED, e.getMessage(), e);
            }
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(S3_CANNOT_OPEN_SPLIT, e);
            }
            String message = format("Error opening S3 split %s (offset=%s, length=%s): %s", bucket+key, start, length, e.getMessage());
            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(S3_MISSING_DATA, message, e);
            }
            throw new PrestoException(S3_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    public static TupleDomain<ColumnDescriptor> getParquetTupleDomain(Map<List<String>, RichColumnDescriptor> descriptorsByPath, TupleDomain<S3ColumnHandle> effectivePredicate)
    {

        ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
        for (Map.Entry<S3ColumnHandle, Domain> entry : effectivePredicate.getDomains().get().entrySet()) {
            S3ColumnHandle columnHandle = entry.getKey();

            RichColumnDescriptor descriptor;
            descriptor = descriptorsByPath.get(ImmutableList.of(columnHandle.getName()));
            if (descriptor != null) {
                predicate.put(descriptor, entry.getValue());
            }
        }
        return TupleDomain.withColumnDomains(predicate.build());
    }

    public static Optional<org.apache.parquet.schema.Type> getParquetType(Type prestoType, MessageType messageType, boolean useParquetColumnNames, S3ColumnHandle column, SchemaTableName tableName, String bucketObjectName)
    {
        org.apache.parquet.schema.Type type = null;
        if (useParquetColumnNames) {
            type = getParquetTypeByName(column.getName(), messageType);
        }
        else if (column.getOrdinalPosition() < messageType.getFieldCount()) {
            type = messageType.getType(column.getOrdinalPosition());
        }

        if (type == null) {
            return Optional.empty();
        }

        if (!checkSchemaMatch(type, prestoType)) {
            String parquetTypeName;
            if (type.isPrimitive()) {
                parquetTypeName = type.asPrimitiveType().getPrimitiveTypeName().toString();
            }
            else {
                GroupType group = type.asGroupType();
                StringBuilder builder = new StringBuilder();
                group.writeToStringBuilder(builder, "");
                parquetTypeName = builder.toString();
            }
            throw new PrestoException(S3_PARTITION_SCHEMA_MISMATCH, format("The column %s of table %s is declared as type %s, but the Parquet file (%s) declares the column as type %s",
                    column.getName(),
                    tableName.toString(),
                    column.getType(),
                    bucketObjectName,
                    parquetTypeName));
        }
        return Optional.of(type);
    }

    private static boolean checkSchemaMatch(org.apache.parquet.schema.Type parquetType, Type type)
    {
        String prestoType = type.getTypeSignature().getBase();
        if (parquetType instanceof GroupType) {
            GroupType groupType = parquetType.asGroupType();
            switch (prestoType) {
                case ROW:
                    RowType rowType = (RowType) type;
                    Map<String, Type> prestoFieldMap = rowType.getFields().stream().collect(
                            Collectors.toMap(
                                    field -> field.getName().get().toLowerCase(Locale.ENGLISH),
                                    field -> field.getType()));
                    for (int i = 0; i < groupType.getFields().size(); i++) {
                        org.apache.parquet.schema.Type parquetFieldType = groupType.getFields().get(i);
                        String fieldName = parquetFieldType.getName().toLowerCase(Locale.ENGLISH);
                        Type prestoFieldType = prestoFieldMap.get(fieldName);
                        if (prestoFieldType != null && !checkSchemaMatch(parquetFieldType, prestoFieldType)) {
                            return false;
                        }
                    }
                    return true;
                case MAP:
                    if (groupType.getFields().size() != 1) {
                        return false;
                    }
                    org.apache.parquet.schema.Type mapKeyType = groupType.getFields().get(0);
                    if (mapKeyType instanceof GroupType) {
                        GroupType mapGroupType = mapKeyType.asGroupType();
                        return mapGroupType.getFields().size() == 2 &&
                                checkSchemaMatch(mapGroupType.getFields().get(0), type.getTypeParameters().get(0)) &&
                                checkSchemaMatch(mapGroupType.getFields().get(1), type.getTypeParameters().get(1));
                    }
                    return false;
                case ARRAY:
                    /* array has a standard 3-level structure with middle level repeated group with a single field:
                     *  optional group my_list (LIST) {
                     *     repeated group element {
                     *        required type field;
                     *     };
                     *  }
                     *  Backward-compatibility support for 2-level arrays:
                     *   optional group my_list (LIST) {
                     *      repeated type field;
                     *   }
                     *  field itself could be primitive or group
                     */
                    if (groupType.getFields().size() != 1) {
                        return false;
                    }
                    org.apache.parquet.schema.Type bagType = groupType.getFields().get(0);
                    if (bagType.isPrimitive()) {
                        return checkSchemaMatch(bagType.asPrimitiveType(), type.getTypeParameters().get(0));
                    }
                    GroupType bagGroupType = bagType.asGroupType();
                    return checkSchemaMatch(bagGroupType, type.getTypeParameters().get(0)) ||
                            (bagGroupType.getFields().size() == 1 && checkSchemaMatch(bagGroupType.getFields().get(0), type.getTypeParameters().get(0)));
                default:
                    return false;
            }
        }

        checkArgument(parquetType.isPrimitive(), "Unexpected parquet type for column: %s " + parquetType.getName());
        PrimitiveType.PrimitiveTypeName parquetTypeName = parquetType.asPrimitiveType().getPrimitiveTypeName();
        switch (parquetTypeName) {
            case INT64:
                return prestoType.equals(BIGINT) || prestoType.equals(DECIMAL) || prestoType.equals(TIMESTAMP);
            case INT32:
                return prestoType.equals(INTEGER) || prestoType.equals(BIGINT) || prestoType.equals(SMALLINT) || prestoType.equals(DATE) || prestoType.equals(DECIMAL) || prestoType.equals(TINYINT);
            case BOOLEAN:
                return prestoType.equals(StandardTypes.BOOLEAN);
            case FLOAT:
                return prestoType.equals(REAL);
            case DOUBLE:
                return prestoType.equals(StandardTypes.DOUBLE);
            case BINARY:
                return prestoType.equals(VARBINARY) || prestoType.equals(VARCHAR) || prestoType.startsWith(CHAR) || prestoType.equals(DECIMAL);
            case INT96:
                return prestoType.equals(TIMESTAMP);
            case FIXED_LEN_BYTE_ARRAY:
                return prestoType.equals(DECIMAL);
            default:
                throw new IllegalArgumentException("Unexpected parquet type name: " + parquetTypeName);
        }
    }

    public static Optional<org.apache.parquet.schema.Type> getColumnType(Type prestoType, MessageType messageType, boolean useParquetColumnNames, S3ColumnHandle column, SchemaTableName tableName, String bucketObjectName)
    {
        return getParquetType(prestoType, messageType, useParquetColumnNames, column, tableName, bucketObjectName);
    }
}
