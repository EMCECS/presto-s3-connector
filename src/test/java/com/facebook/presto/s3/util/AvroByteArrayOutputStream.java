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
package com.facebook.presto.s3.util;

import java.io.ByteArrayOutputStream;

/**
 * helper to check how many blocks have been written to avro data.
 */
public class AvroByteArrayOutputStream
        extends ByteArrayOutputStream {

    private final byte[] sync;

    private int syncs = 0;

    public AvroByteArrayOutputStream(byte[] sync) {
        this.sync = sync;
    }

    @Override
    public void write(byte[] b, int off, int len) {
        if (len == sync.length) {
            int i = 0;
            for (; i < sync.length; i++) {
                if (b[off + i] != sync[i]) {
                    break;
                }
            }
            if (i == sync.length) {
                syncs++;
            }
        }
        super.write(b, off, len);
    }

    public int getSyncs() {
        return syncs;
    }
}
