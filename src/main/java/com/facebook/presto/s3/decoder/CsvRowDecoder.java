/*
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
package com.facebook.presto.s3.csv;

import au.com.bytecode.opencsv.CSVParser;
import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * Decode row as CSV. This is an extremely primitive CSV decoder using {@link au.com.bytecode.opencsv.CSVParser]}.
 */
public class CsvRowDecoder
        implements RowDecoder
{
    public static final String NAME = "csv";

    private final Map<DecoderColumnHandle, CsvColumnDecoder> columnDecoders;
    private CSVParser parser = new CSVParser();

    public CsvRowDecoder(Set<DecoderColumnHandle> columnHandles)
    {
        requireNonNull(columnHandles, "columnHandles is null");
        columnDecoders = columnHandles.stream()
                .collect(toImmutableMap(identity(), this::createColumnDecoder));
    }

    private CsvColumnDecoder createColumnDecoder(DecoderColumnHandle columnHandle)
    {
        return new CsvColumnDecoder(columnHandle);
    }

    public void setFieldDelimiter(char newDelimiter)
    {
        parser = new CSVParser(newDelimiter);
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data, Map<String, String> dataMap)
    {
        String[] tokens;
        try {
            // TODO - There is no reason why the row can't have a formatHint and it could be used
            // to set the charset here.
            String line = new String(data, StandardCharsets.UTF_8);
            tokens = parser.parseLine(line);
        }
        catch (Exception e) {
            return Optional.empty();
        }

        return Optional.of(columnDecoders.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().decodeField(tokens))));
    }
}
