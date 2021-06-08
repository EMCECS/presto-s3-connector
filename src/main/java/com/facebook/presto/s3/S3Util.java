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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import io.airlift.units.DataSize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.LinkedList;
import java.util.List;

import static com.facebook.presto.s3.S3Const.*;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class S3Util
{
    private S3Util()
    {
    }

    public static byte[] serialize(Serializable s)
    {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(s);
            return baos.toByteArray();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <T> T deserialize(byte[] bytes, Class<T> clazz)
    {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream bis = new ObjectInputStream(bais);
            return (T) bis.readObject();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static List<PropertyMetadata<?>> buildSessionPropertyList()
    {
        List<PropertyMetadata<?>> propertyMetadataList = new LinkedList<>();

        propertyMetadataList.add(
                PropertyMetadata.booleanProperty(
                        SESSION_PROP_S3_SELECT_PUSHDOWN,
                        "enable/disable s3 select pushdown",
                        Boolean.FALSE,
                        false));

        propertyMetadataList.add(
                PropertyMetadata.integerProperty(
                        SESSION_PROP_READER_BUFFER_SIZE_BYTES,
                        "stream reader buffer size bytes",
                        65536,
                        false));

        propertyMetadataList.add(
                PropertyMetadata.booleanProperty(
                        SESSION_PROP_NEW_RECORD_CURSOR,
                        "use new record cursor",
                        Boolean.TRUE,
                        false));

        propertyMetadataList.add(
                PropertyMetadata.booleanProperty(
                        PARQUET_USE_COLUMN_NAME,
                        "Experimental: Parquet: Access Parquet columns using names from the file",
                        Boolean.FALSE,
                        false));

        propertyMetadataList.add(
                PropertyMetadata.booleanProperty(
                        PARQUET_BATCH_READ_OPTIMIZATION_ENABLED,
                        "Is Parquet batch read optimization enabled",
                        Boolean.TRUE,
                        false));

        propertyMetadataList.add(
                PropertyMetadata.booleanProperty(
                        PARQUET_BATCH_READER_VERIFICATION_ENABLED,
                        "Is Parquet batch reader verification enabled? This is for testing purposes only, not to be used in production",
                        Boolean.FALSE,
                        false));

        propertyMetadataList.add(
                PropertyMetadata.booleanProperty(
                        PARQUET_FAIL_WITH_CORRUPTED_STATISTICS,
                        "Parquet: Fail when scanning Parquet files with corrupted statistics",
                        Boolean.TRUE,
                        false));

        propertyMetadataList.add(
                PropertyMetadata.dataSizeProperty(
                        PARQUET_MAX_READ_BLOCK_SIZE,
                        "Parquet: Maximum size of a block to read",
                        new DataSize(16, MEGABYTE),
                        false));

        propertyMetadataList.add(
                PropertyMetadata.integerProperty(
                        SESSION_PROP_SPLIT_BATCH,
                        "max splits to return per batch",
                        100,
                        false));

        propertyMetadataList.add(
                PropertyMetadata.integerProperty(
                        SESSION_PROP_SPLIT_PAUSE_MS,
                        "throttle ms",
                        10,
                        false));

        propertyMetadataList.add(
                PropertyMetadata.integerProperty(
                        SESSION_PROP_SPLIT_RANGE_MB,
                        "max split size in mb",
                        32,
                        false));

        return propertyMetadataList;
    }

    public static S3ReaderProps constructReaderProps(ConnectorSession session)
    {
        return new S3ReaderProps(boolProp(session, SESSION_PROP_S3_SELECT_PUSHDOWN, false),
                intProp(session, SESSION_PROP_READER_BUFFER_SIZE_BYTES, 65536));
    }

    public static boolean boolProp(ConnectorSession session, String prop, boolean dflt)
    {
        try {
            return session.getProperty(prop, Boolean.class);
        }
        catch (PrestoException e) {
            return dflt;
        }
    }

    public static int intProp(ConnectorSession session, String prop, int dflt)
    {
        try {
            return session.getProperty(prop, Integer.class);
        }
        catch (PrestoException e) {
            return dflt;
        }
    }

    public static boolean s3SelectEnabled(ConnectorSession session)
    {
        try {
            return session.getProperty(SESSION_PROP_S3_SELECT_PUSHDOWN, Boolean.class);
        }
        catch (PrestoException e) {
            return false;
        }
    }
}
