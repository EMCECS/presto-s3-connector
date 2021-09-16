package com.facebook.presto.s3;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class FieldValueProviderRecord
        implements S3Record
{
    private final FieldValueProvider[] currentRowValues;

    private Optional<Map<DecoderColumnHandle, FieldValueProvider>> fieldValueProviderMap;

    public FieldValueProviderRecord(List<S3ColumnHandle> columnHandles)
    {
        this.currentRowValues = new FieldValueProvider[columnHandles.size()];
    }

    @Override
    public void decode()
    {

    }

    public void setDecodedValue(Optional<Map<DecoderColumnHandle, FieldValueProvider>> fieldValueProviderMap)
    {
        this.fieldValueProviderMap = fieldValueProviderMap;
    }

    @Override
    public boolean isNull(int field)
    {
        return false;
    }

    @Override
    public Long getLong(int field)
    {
        return null;
    }

    @Override
    public Double getDouble(int field)
    {
        return null;
    }

    @Override
    public Boolean getBoolean(int field)
    {
        return null;
    }

    @Override
    public Slice getSlice(int field)
    {
        return null;
    }
}
