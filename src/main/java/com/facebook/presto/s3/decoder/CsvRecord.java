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
    private final Position[] position;

    private final char fieldSep;

    public boolean decoded = false;

    private static final byte QUOTE = '"';

    private static class Position {
        int pos;
        int len;
        Position(int pos, int len) {
            this.pos = pos;
            this.len = len;
        }
    }

    public CsvRecord(char fieldSep)
    {
        this.fieldSep = fieldSep;

        this.len = 0;
        this.value = new byte[65536];
        this.position = new Position[1024];
    }

    /*
    private String debug(int field) {
        return "{" + position[field].pos + "," + position[field].len + "}: " +
                getSlice(field).toStringUtf8();
    }
     */

    public void decode()
    {
        positions = 0;

        int idx = 0;

        boolean quoted = false;

        int p = 0;
        int l;

        // look for non-quoted field separator
        // note offset + length of each field
        // if value starts+ends with quote, trim quotes
        while (idx < len) {
            // terminate field with separator or end of line
            if ((value[idx] == fieldSep && !quoted) ||
                    idx+1 == len) {

                l = idx - p + (value[idx] == fieldSep ? 0 : 1);

                if (value[p] == QUOTE && value[p+l-1] == QUOTE) {
                    p++;
                    l-=2; // backup before quote and account for p++
                }

                position[positions++] = new Position(p, l);
                p = idx + 1; // +1 skip field sep
            } else if (value[idx] == QUOTE) {
                quoted = !quoted;
            }

            idx++;
        }

        decoded = true;
    }

    public boolean isNull(int field)
    {
        if (!decoded) {
            decode();
        }
        return field >= positions || position[field].len == 0;
    }

    public long getLong(int field)
    {
        if (!decoded) {
            decode();
        }
        return Long.parseLong(new String(value, position[field].pos, position[field].len));
    }

    public double getDouble(int field)
    {
        if (!decoded) {
            decode();
        }
        return Double.parseDouble(new String(value, position[field].pos, position[field].len));
    }

    public boolean getBoolean(int field)
    {
        if (!decoded) {
            decode();
        }
        return Boolean.parseBoolean(new String(value, position[field].pos, position[field].len));
    }

    public Slice getSlice(int field)
    {
        if (!decoded) {
            decode();
        }
        return Slices.wrappedBuffer(value, position[field].pos, position[field].len);
    }
}
