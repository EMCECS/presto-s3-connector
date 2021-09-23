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
import com.facebook.presto.s3.CountingInputStream;
import com.facebook.presto.s3.S3ColumnHandle;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.google.common.base.Functions.identity;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class AvroRecordReader
        implements RecordReader
{
    private final Map<DecoderColumnHandle, AvroColumnDecoder> columnDecoders;

    private final Supplier<CountingInputStream> inputStreamSupplier;

    private DataFileStream<GenericRecord> reader = null;

    private CountingInputStream inputStream;

    public AvroRecordReader(List<S3ColumnHandle> columnHandles, final Supplier<CountingInputStream> inputStreamSupplier)
    {
        this.columnDecoders = columnHandles.stream().collect(toImmutableMap(identity(), this::createColumnDecoder));
        this.inputStreamSupplier = inputStreamSupplier;
    }

    private AvroColumnDecoder createColumnDecoder(DecoderColumnHandle columnHandle)
    {
        return new AvroColumnDecoder(columnHandle);
    }

    private void init()
    {
        try {
            this.inputStream = inputStreamSupplier.get();
            this.reader = new DataFileStream<>(inputStream, new SpecificDatumReader<>());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getTotalBytes()
    {
        return inputStream == null
                ? 0
                : inputStream.getTotalBytes();
    }

    @Override
    public boolean hasNext()
    {
        if (reader == null) {
            init();
        }

        return reader.hasNext();
    }

    @Override
    public Map<DecoderColumnHandle, FieldValueProvider> next()
    {
        if (reader == null) {
            init();
        }

        final GenericRecord record = reader.next();

        return columnDecoders.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().decodeField(record)));
    }

    @Override
    public void close()
    {
        if (reader != null) {
            try {
                reader.close();
            }
            catch (IOException ignore) {}
        }
    }
}
