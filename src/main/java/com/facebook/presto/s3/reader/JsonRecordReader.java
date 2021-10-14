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
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.s3.BytesLineReader;
import com.facebook.presto.s3.CountingInputStream;
import com.facebook.presto.s3.S3ObjectRange;
import com.facebook.presto.s3.S3ReaderProps;

import java.io.*;
import java.util.*;
import java.util.function.Supplier;

public class JsonRecordReader
        implements RecordReader {

    private final RowDecoder rowDecoder;

    private final Supplier<CountingInputStream> inputStreamSupplier;

    private CountingInputStream inputStream;

    private final S3ObjectRange objectRange;

    private BytesLineReader bytesLineReader = null;

    private int length;

    private byte[] line;

    private final int bufferSize;

    public JsonRecordReader(RowDecoder rowDecoder, S3ReaderProps readerProps, S3ObjectRange objectRange, final Supplier<CountingInputStream> inputStreamSupplier)
    {
        this.rowDecoder = rowDecoder;
        this.objectRange = objectRange;
        this.inputStreamSupplier = inputStreamSupplier;
        this.bufferSize = readerProps.getBufferSizeBytes();
    }

    private void init() {
        this.line = new byte[bufferSize];
        this.inputStream = inputStreamSupplier.get();
        this.bytesLineReader = new BytesLineReader(inputStream,
                bufferSize,
                objectRange.getOffset(),
                objectRange.getOffset() + objectRange.getLength());
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
        if (bytesLineReader == null) {
            init();
        }

        length = bytesLineReader.read(line);
        return length >= 0;
    }

    @Override
    public Map<DecoderColumnHandle, FieldValueProvider> next()
    {
        byte[] row = new byte[length];
        System.arraycopy(line, 0, row, 0, length);

        // FIXME
        Optional<Map<DecoderColumnHandle, FieldValueProvider>> fieldValueProviderMap =
                rowDecoder.decodeRow(line, Collections.EMPTY_MAP);
        return fieldValueProviderMap.get();
    }

    @Override
    public void close()
    {
        try {
            inputStream.close();
        }
        catch (IOException ignore) {}
    }
}
