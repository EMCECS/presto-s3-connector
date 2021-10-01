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

import com.facebook.presto.s3.BytesLineReader;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class CsvRecordTest {

    static class CsvTest {
        CsvRecord record = new CsvRecord(',');
        List<String> expected = new ArrayList<>();
    }

    @Test
    public void testCsv() {
        runTest("quoted.csv");
        runTest("quickbrownfox.csv");
        runTest("emptyfields.csv");
        runTest("unmatchedquoteatend.csv");
        runTest("unmatchedquoteinmiddle.csv");

        runTest("\"value1\",\"value2\"", Stream.of("value1", "value2"));
    }

    private void runTest(String f) {
        verifyTest(readTest(f));
    }

    private void runTest(String line, Stream<String> expected) {
        CsvTest test = new CsvTest();
        byte[] buf = line.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(buf, 0, test.record.value, 0, buf.length);
        test.record.len = buf.length;
        test.expected.addAll(expected.collect(Collectors.toList()));

        verifyTest(test);
    }

    private void verifyTest(CsvTest test) {
        test.record.decode();

        assertTrue(test.record.positions > 0);
        assertEquals(test.record.positions, test.expected.size());

        for (int i = 0; i < test.record.positions; i++) {
            assertEquals(test.record.getSlice(i).toStringUtf8(), test.expected.get(i));
        }
    }

    private CsvTest readTest(String f) {
        // test file format is
        // 1st line: full csv line
        // 2nd -> Nth line are expected results, each line is a field, for example:
        //
        // one,two,three
        // one
        // two
        // three

        int lines = 0;
        byte[] buf = new byte[4096];
        CsvTest test = new CsvTest();

        try (BytesLineReader reader =
                     new BytesLineReader(CsvRecordTest.class.getResourceAsStream(String.format("/csvRecordTest/%s", f)))) {
            while (true) {
                int len = reader.read(buf);
                if (len < 0) {
                    break;
                }
                if (lines++ == 0) {
                    System.arraycopy(buf, 0, test.record.value, 0, len);
                    test.record.len = len;
                } else {
                    test.expected.add(new String(buf, 0, len));
                }
            }
        }
        return test;
    }
}
