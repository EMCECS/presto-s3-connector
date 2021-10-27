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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;

public class SeekableFileStream implements SeekableStream {

    private final RandomAccessFile file;

    public SeekableFileStream(File f) {
        try {
            this.file = new RandomAccessFile(f, "r");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void seek(long l) {
        try {
            this.file.seek(l);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int read(byte[] b, int off, int len) {
        try {
            return file.read(b, off, len);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
