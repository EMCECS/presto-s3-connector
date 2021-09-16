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
package com.facebook.presto.s3.reader;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.avro.AvroColumnDecoder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Functions.identity;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class AvroRecordReader
        implements RecordReader
{
    private final Map<DecoderColumnHandle, AvroColumnDecoder> columnDecoders;

    private final DataFileStream<GenericRecord> reader;

    public AvroRecordReader(Set<DecoderColumnHandle> columnHandles, final InputStream inputStream) throws IOException
    {
        this.columnDecoders = columnHandles.stream().collect(toImmutableMap(identity(), this::createColumnDecoder));
        this.reader = new DataFileStream<>(inputStream, new SpecificDatumReader<>());
    }

    private AvroColumnDecoder createColumnDecoder(DecoderColumnHandle columnHandle)
    {
        return new AvroColumnDecoder(columnHandle);
    }

    @Override
    public boolean hasNext()
    {
        return reader.hasNext();
    }

    @Override
    public Map<DecoderColumnHandle, FieldValueProvider> next()
    {
        final GenericRecord record = reader.next();

        return columnDecoders.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().decodeField(record)));
    }

    @Override
    public void close()
    {
    }
}
