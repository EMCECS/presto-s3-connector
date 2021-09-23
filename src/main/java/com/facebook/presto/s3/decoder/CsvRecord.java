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
    private boolean quoted = false;

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

    public void decode()
    {
        positions = 0;

        int absPos = 0;
        int absPrevPos = 0;

        int p;
        int l;

        // look for non-quoted field seps
        // note offset + length of each field (+1's are to skip field sep in byte byte[])
        while (absPos < len) {
            if (value[absPos] == fieldSep && !quoted) {
                p = positions == 0 ? 0 : absPrevPos + 1;
                l = absPos - absPrevPos - (positions == 0 ? 0 : 1);
                position[positions++] = new Position(p, l);
                absPrevPos = absPos;

            } else if (value[absPos] == QUOTE) {
                quoted = !quoted;
            }
            absPos++;
        }

        p = positions == 0 ? 0 : absPrevPos + 1;
        l = absPos - absPrevPos - (positions == 0 ? 0 : 1);
        position[positions++] = new Position(p, l);

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
        return Boolean.getBoolean(new String(value, position[field].pos, position[field].len));
    }

    public Slice getSlice(int field)
    {
        if (!decoded) {
            decode();
        }
        return Slices.wrappedBuffer(value, position[field].pos, position[field].len);
    }
}
