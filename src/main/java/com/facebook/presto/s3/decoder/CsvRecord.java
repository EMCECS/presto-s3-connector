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

package com.facebook.presto.s3.decoder;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

// TODO: https://github.com/EMCECS/presto-s3-connector/issues/27
public class CsvRecord
{
    public int len;
    public byte[] value;

    int positions;
    int[] position;

    char fieldSep;

    public boolean decoded = false;

    public CsvRecord(char fieldSep)
    {
        this.fieldSep = fieldSep;

        this.len = 0;
        this.value = new byte[65536];
        this.position = new int[1024];
    }

    public void decode()
    {
        position[0] = 0;
        positions = 1;

        int pos = 0;
        while (pos < len) {
            if (value[pos++] == fieldSep) {
                position[positions++] = pos;
            }
        }

        decoded = true;
    }

    private int fieldLen(int field)
    {
        return field+1 == positions
                ? len - position[field]
                : position[field+1] - position[field] - 1;
    }

    public boolean isNull(int field)
    {
        if (!decoded) {
            decode();
        }
        return field >= positions || fieldLen(field) == 0;
    }

    public long getLong(int field)
    {
        if (!decoded) {
            decode();
        }
        return Long.parseLong(new String(value, position[field], fieldLen(field)));
    }

    public double getDouble(int field)
    {
        if (!decoded) {
            decode();
        }
        return Double.parseDouble(new String(value, position[field], fieldLen(field)));
    }

    public boolean getBoolean(int field)
    {
        if (!decoded) {
            decode();
        }
        return Boolean.getBoolean(new String(value, position[field], fieldLen(field)));
    }

    public Slice getSlice(int field)
    {
        if (!decoded) {
            decode();
        }
        return Slices.wrappedBuffer(value, position[field], fieldLen(field));
    }
}
