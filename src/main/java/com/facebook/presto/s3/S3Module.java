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

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.decoder.DecoderModule;
import com.facebook.presto.s3.parquet.ParquetPageSourceFactory;
import com.facebook.presto.parquet.cache.*;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.testing.TestingMBeanServer;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.management.MBeanServer;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.util.Objects.requireNonNull;

public class S3Module
        extends AbstractConfigurationAwareModule
 {
    private final String connectorId;
    public S3Module(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
    }

    @Override
    public void setup(Binder binder)
    {
        binder.bind(S3ConnectorId.class).toInstance(new S3ConnectorId(connectorId));
        binder.bind(S3Metadata.class).in(Scopes.SINGLETON);
        binder.bind(S3ObjectManager.class).in(Scopes.SINGLETON);
        binder.bind(S3SplitManager.class).in(Scopes.SINGLETON);
        binder.bind(MBeanServer.class).toInstance(new TestingMBeanServer());
        binder.bind(MBeanExporter.class).in(Scopes.SINGLETON);
        binder.bind(S3AccessObject.class).in(Scopes.SINGLETON);
        binder.bind(S3PageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(S3PageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(S3SchemaRegistryManager.class).in(Scopes.SINGLETON);
        Multibinder<S3BatchPageSourceFactory> pageSourceFactoryBinder = newSetBinder(binder, S3BatchPageSourceFactory.class);
        pageSourceFactoryBinder.addBinding().to(ParquetPageSourceFactory.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(S3ConnectorConfig.class);
        configBinder(binder).bindConfig(ParquetCacheConfig.class, connectorId);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindJsonCodec(S3Table.class);

        binder.install(new DecoderModule());
    }
     @Singleton
     @Provides
     public ParquetMetadataSource createParquetMetadataSource()
     {
         ParquetMetadataSource parquetMetadataSource = new MetadataReader();
         return parquetMetadataSource;
     }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = typeManager.getType(parseTypeSignature(value));
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }
}
