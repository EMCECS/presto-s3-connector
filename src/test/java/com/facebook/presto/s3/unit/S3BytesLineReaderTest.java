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

package com.facebook.presto.s3.unit;

import com.facebook.presto.s3.*;

import org.testng.annotations.Test;
import java.io.ByteArrayInputStream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class S3BytesLineReaderTest
{
    @Test
    public void testParseLine()
    {
        int len;
        String line;
        byte[] lineBuf = new byte[1024];
        byte[] data = new byte[] {'a', 'n', 'd', 'r', 'e', 'w', '\n'};
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);

        BytesLineReader reader = new BytesLineReader(inputStream);

        len = reader.read(lineBuf);
        line = new String(lineBuf, 0, len);
        assertEquals("andrew", line);

        assertEquals(-1, reader.read(lineBuf));
    }

    @Test
    public void testParseLineMulti()
    {
        int len;
        String line;
        byte[] lineBuf = new byte[1024];
        byte[] data = new byte[] {'a', 'n', 'd', 'r', 'e', 'w', '\n', 't', 'i', 'm', '\n'};
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);

        BytesLineReader reader = new BytesLineReader(inputStream);

        len = reader.read(lineBuf);
        line = new String(lineBuf, 0, len);
        assertEquals("andrew", line);

        len = reader.read(lineBuf);
        line = new String(lineBuf, 0, len);
        assertEquals("tim", line);

        assertEquals(-1, reader.read(lineBuf));
    }

    @Test
    public void testParseRequireRefill()
    {
        int len;
        String line;
        byte[] lineBuf = new byte[1024];
        byte[] data = new byte[] {'a', 'n', 'd', 'r', 'e', 'w', '\n', 't', 'i', 'm', '\n'};
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);

        // buf size of 2 so we have to refill a couple of times
        BytesLineReader reader = new BytesLineReader(inputStream, 2);

        len = reader.read(lineBuf);
        line = new String(lineBuf, 0, len);
        assertEquals("andrew", line);

        len = reader.read(lineBuf);
        line = new String(lineBuf, 0, len);
        assertEquals("tim", line);

        assertEquals(-1, reader.read(lineBuf));
    }

    @Test
    public void testParseMixEndOfRecordSep()
    {
        int len;
        String line;
        byte[] lineBuf = new byte[1024];
        byte[] data = new byte[] {'a', 'n', 'd', 'r', 'e', 'w', '\n',
                't', 'i', 'm', '\r', '\n',
                'c', 'h', 'a', 'r', 'l', 'e', 's', '\r', '\n'};
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);

        // buf size of 2 so we have to refill a couple of times
        BytesLineReader reader = new BytesLineReader(inputStream, 2);

        len = reader.read(lineBuf);
        line = new String(lineBuf, 0, len);
        assertEquals("andrew", line);

        len = reader.read(lineBuf);
        line = new String(lineBuf, 0, len);
        assertEquals("tim", line);

        len = reader.read(lineBuf);
        line = new String(lineBuf, 0, len);
        assertEquals("charles", line);

        assertEquals(-1, reader.read(lineBuf));
    }

    @Test
    public void testEmptyLine()
    {
        int len;
        String line;
        byte[] lineBuf = new byte[1024];
        byte[] data = new byte[] {'\n'};
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);

        BytesLineReader reader = new BytesLineReader(inputStream);

        len = reader.read(lineBuf);
        line = new String(lineBuf, 0, len);
        assertEquals("", line);

        assertEquals(-1, reader.read(lineBuf));
    }

    @Test
    public void testNonZeroStart()
    {
        int len;
        String line;
        byte[] lineBuf = new byte[1024];
        byte[] data = new byte[] {'x', 'y', 'z', '\n', 'a', 'n', 'd', 'r', 'e', 'w', '\n'};
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);

        BytesLineReader reader = new BytesLineReader(inputStream, 2, 1, Long.MAX_VALUE);

        len = reader.read(lineBuf);
        line = new String(lineBuf, 0, len);
        assertEquals("andrew", line);
    }

    @Test
    public void testNonMaxEnd()
    {
        int len;
        String line;
        byte[] lineBuf = new byte[1024];
        byte[] data = new byte[] {'x', 'y', 'z', '\n', 'a', 'n', 'd', 'r', 'e', 'w', '\n'};
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);

        BytesLineReader reader = new BytesLineReader(inputStream, 2, 0, 7);

        len = reader.read(lineBuf);
        line = new String(lineBuf, 0, len);
        assertEquals("xyz", line);

        len = reader.read(lineBuf);
        line = new String(lineBuf, 0, len);
        assertEquals("andrew", line);
    }

    @Test
    public void testPartial()
    {
        int len;
        String line;
        byte[] lineBuf = new byte[1024];
        byte[] data = new byte[] {'a', 'n', 'd', 'r'};
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);

        BytesLineReader reader = new BytesLineReader(inputStream);

        len = reader.read(lineBuf);
        assertTrue(len > 0);
        line = new String(lineBuf, 0, len);
        assertEquals("andr", line);
    }
}
