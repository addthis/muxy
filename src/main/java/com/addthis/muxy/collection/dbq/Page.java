/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.addthis.muxy.collection.dbq;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.addthis.muxy.MuxFileDirectory;
import com.addthis.muxy.WritableMuxFile;
import com.addthis.muxy.collection.Serializer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;

/**
 * Fixed length circular buffer of elements.
 */
class Page<E> {

    final long id;

    final Object[] elements;

    final int pageSize;

    final Serializer<E> serializer;

    @GuardedBy("external")
    private final MuxFileDirectory external;

    int readerIndex;

    int writerIndex;

    int count;

    Page(long id, int pageSize, Serializer<E> serializer, MuxFileDirectory external,
         InputStream stream) throws IOException {
        try {
            this.id = id;
            this.pageSize = pageSize;
            this.serializer = serializer;
            this.external = external;
            this.elements = new Object[pageSize];
            this.count = readInt(stream);
            for (int i = 0; i < count; i++) {
                elements[i] = serializer.fromInputStream(stream);
            }
            this.readerIndex = 0;
            this.writerIndex = count;
        } finally {
            stream.close();
        }
    }

    Page(long id, int pageSize, Serializer<E> serializer, MuxFileDirectory external) {
        this.id = id;
        this.pageSize = pageSize;
        this.serializer = serializer;
        this.external = external;
        this.elements = new Object[pageSize];
        this.count = 0;
        this.readerIndex = 0;
        this.writerIndex = 0;
    }

    boolean empty() {
        return (count == 0);
    }

    boolean full() {
        return (count == pageSize);
    }

    void add(E e) {
        assert(!full());
        elements[writerIndex] = e;
        writerIndex = (writerIndex + 1) % pageSize;
        count++;
    }

    void clear() {
        count = 0;
        readerIndex = 0;
        writerIndex = 0;
    }

    @SuppressWarnings("unchecked")
    E remove() {
        assert(!empty());
        E result = (E) elements[readerIndex];
        readerIndex = (readerIndex + 1) % pageSize;
        count--;
        return result;
    }

    @SuppressWarnings("unchecked")
    void writeToFile() throws IOException {
        assert(!empty());
        synchronized (external) {
            WritableMuxFile file = external.openFile(Long.toString(id), true);
            assert(file.getLength() == 0);
            try (OutputStream outputStream = file.append()) {
                writeInt(outputStream, count);
                for (int i = 0; i < count; i++) {
                    E next = (E) elements[(readerIndex + i) % pageSize];
                    serializer.toOutputStream(next, outputStream);
                }
            } finally {
                file.sync();
            }
        }
    }

    @VisibleForTesting
    static int readInt(InputStream stream) throws IOException {
        byte[] data = new byte[4];
        ByteStreams.readFully(stream, data);
        return Ints.fromByteArray(data);
    }

    @VisibleForTesting
    static void writeInt(OutputStream stream, int val) throws IOException {
        byte[] data = Ints.toByteArray(val);
        stream.write(data);
    }
}
