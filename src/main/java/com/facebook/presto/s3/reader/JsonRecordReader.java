package com.facebook.presto.s3.reader;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class JsonRecordReader
        implements RecordReader {

    private final RowDecoder rowDecoder;

    private final Iterator<String> lineIterator;

    public JsonRecordReader(RowDecoder rowDecoder, final InputStream inputStream)
    {
        this.rowDecoder = rowDecoder;
        this.lineIterator = tempS3ObjectToStringObjectList(inputStream).iterator();
    }

    @Override
    public boolean hasNext()
    {
        return lineIterator.hasNext();
    }

    @Override
    public Map<DecoderColumnHandle, FieldValueProvider> next()
    {
        Optional<Map<DecoderColumnHandle, FieldValueProvider>> fieldValueProviderMap =
                rowDecoder.decodeRow(lineIterator.next().getBytes(StandardCharsets.UTF_8), Collections.EMPTY_MAP);
        return fieldValueProviderMap.get();
    }

    @Override
    public void close()
    {
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
