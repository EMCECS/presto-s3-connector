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
import com.facebook.presto.s3.CountingInputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Supplier;

public class JsonRecordReader
        implements RecordReader {

    private final RowDecoder rowDecoder;

    private Iterator<String> lineIterator;

    private final Supplier<CountingInputStream> inputStreamSupplier;

    private CountingInputStream inputStream;

    public JsonRecordReader(RowDecoder rowDecoder, final Supplier<CountingInputStream> inputStreamSupplier)
    {
        this.rowDecoder = rowDecoder;
        this.inputStreamSupplier = inputStreamSupplier;
    }

    private void init() {
        this.inputStream = inputStreamSupplier.get();
        this.lineIterator = tempS3ObjectToStringObjectList(inputStream).iterator();
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
        if (lineIterator == null) {
            init();
        }

        return lineIterator.hasNext();
    }

    @Override
    public Map<DecoderColumnHandle, FieldValueProvider> next()
    {
        if (lineIterator == null) {
            init();
        }

        Optional<Map<DecoderColumnHandle, FieldValueProvider>> fieldValueProviderMap =
                rowDecoder.decodeRow(lineIterator.next().getBytes(StandardCharsets.UTF_8), Collections.EMPTY_MAP);
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

    private ArrayList<String> tempS3ObjectToStringObjectList(InputStream inputStream)
    {
        // i don't think can stream with JSONObject. For now read it all.
        ArrayList<String> json = new ArrayList<>();
        String line;
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        try {
            while ((line = bufferedReader.readLine()) != null){
                json.add(line);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return json;
    }
}
