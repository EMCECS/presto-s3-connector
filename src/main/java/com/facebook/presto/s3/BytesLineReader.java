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
package com.facebook.presto.s3;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

public class BytesLineReader
        implements Closeable
{
    private final InputStream inputStream;
    private boolean skip = false;
    private int bufPos;
    private int bufLen;
    private final byte[] buf;

    private long absPos;
    private final long start;
    private final long end;
    private boolean spillover = false;
    private boolean eof;

    public BytesLineReader(InputStream inputStream)
    {
        this(inputStream, 65536);
    }

    public BytesLineReader(InputStream inputStream, int bufSizeBytes)
    {
        this(inputStream, bufSizeBytes, 0L, Long.MAX_VALUE);
    }

    // TODO: https://github.com/EMCECS/presto-s3-connector/issues/26
    public BytesLineReader(InputStream inputStream, int bufSizeBytes, long start, long end)
    {
        this.inputStream = inputStream;
        this.bufPos = 0;
        this.bufLen = 0;
        this.buf = new byte[bufSizeBytes];
        this.start = start;
        this.end = end;
        this.absPos = start;

        if (start != 0) {
            // seek to first record
            // TODO: https://github.com/EMCECS/presto-s3-connector/issues/26
            byte[] tmp = new byte[Math.max(65536, bufSizeBytes)];
            read(tmp);
            absPos += bufPos;
        }
    }

    public long bytesProcessed()
    {
        return absPos - start;
    }

    private void fill()
    {
        try {
            long toRead = end-absPos <= 0 ? buf.length : end-absPos;
            toRead = Math.min(buf.length, toRead);
            bufPos = 0;
            bufLen = inputStream.read(buf, 0, (int) toRead);
            if (bufLen == -1) {
                eof = true;
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }


    private boolean spillover()
    {
        return !eof && spillover;
    }

    public int read(byte[] value)
    {
        int valuePos = 0;

        while (absPos <= end || spillover()) {
            if (bufPos >= bufLen) {
                fill();
            }

            if (bufPos >= bufLen) {
                return valuePos > 0 ? valuePos : -1;
            }

            if (skip && buf[bufPos] == 10) {
                // matched '\r' from last record, expecting '\n'
                bufPos++;
                absPos++;
            }

            skip = false;

            // search for end of record
            boolean endOfLine = false;
            int idx;
            for (idx = bufPos; idx < bufLen; idx++) {
                if (buf[idx] == 10 || buf[idx] == 13) {
                    endOfLine = true;
                    break;
                }
            }

            int len = idx - bufPos;
            absPos += len;

            if (endOfLine) {
                // found it, copy remaining result and return
                System.arraycopy(buf, bufPos, value, valuePos, len);
                skip = buf[idx] == 13; // '\n' from '\r\n' if present when parsing next line
                bufPos = idx + 1; // +1 skip the current '\n' or '\r'
                absPos++;
                eof = spillover;
                valuePos += len;
                return valuePos;
            }

            // out of buffer but did not find end of record, keep going
            System.arraycopy(buf, bufPos, value, valuePos, len);
            bufPos = idx;
            valuePos += len;

            if (absPos >= end && end != Long.MAX_VALUE) {
                // end offset is arbitrary
                // keep going til end of record in case we haven't found it
                spillover = true;
            }
        }

        return valuePos > 0 ? valuePos : -1;
    }

    public void close()
    {
        try {
            inputStream.close();
        }
        catch (IOException ignore) {
        }
    }
}
