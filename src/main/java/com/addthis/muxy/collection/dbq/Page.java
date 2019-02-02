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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.ByteArrayOutputStream;
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

    private static class ObjectByteArrayPair<E> {
        @Nonnull final E value;
        @Nullable final byte[] bytearray;

        ObjectByteArrayPair(@Nonnull E value, @Nullable byte[] bytearray) {
            this.value = value;
            this.bytearray = bytearray;
        }
    }

    final long id;

    final ObjectByteArrayPair[] elements;

    final int pageSize;

    final Serializer<E> serializer;

    final boolean compress;

    @GuardedBy("external")
    private final MuxFileDirectory external;

    int readerIndex;

    int writerIndex;

    int count;

    Page(long id, int pageSize, Serializer<E> serializer, boolean compress,
         MuxFileDirectory external, InputStream stream) throws IOException {
        try {
            this.id = id;
            this.pageSize = pageSize;
            this.serializer = serializer;
            this.compress = compress;
            this.external = external;
            this.elements = new ObjectByteArrayPair[pageSize];
            this.count = readInt(stream);
            for (int i = 0; i < count; i++) {
                ObjectByteArrayPair<E> pair = new ObjectByteArrayPair<>(serializer.fromInputStream(stream), null);
                elements[i] = pair;
            }
            this.readerIndex = 0;
            this.writerIndex = count;
        } finally {
            stream.close();
        }
    }

    Page(long id, int pageSize, Serializer<E> serializer, boolean compress,
         MuxFileDirectory external) {
        this.id = id;
        this.pageSize = pageSize;
        this.serializer = serializer;
        this.compress = compress;
        this.external = external;
        this.elements = new ObjectByteArrayPair[pageSize];
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

    void add(E e, byte[] bytearray) {
        assert(!full());
        elements[writerIndex] = new ObjectByteArrayPair<>(e, bytearray);
        writerIndex = (writerIndex + 1) % pageSize;
        count++;
    }

    void clear() {
        count = 0;
        readerIndex = 0;
        writerIndex = 0;
    }

    E remove() {
        assert(!empty());
        ObjectByteArrayPair<E> result = elements[readerIndex];
        readerIndex = (readerIndex + 1) % pageSize;
        count--;
        return result.value;
    }

    void writeToFile() throws IOException {
        assert(!empty());
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeInt(output, count);
        for (int i = 0; i < count; i++) {
            ObjectByteArrayPair<E> next = elements[(readerIndex + i) % pageSize];
            if (next.bytearray != null) {
                output.write(next.bytearray);
            } else {
                serializer.toOutputStream(next.value, output);
            }
        }
        synchronized (external) {
            WritableMuxFile file = external.openFile(Long.toString(id), true);
            assert(file.getLength() == 0);
            try (OutputStream outputStream = file.append(compress)) {
                output.writeTo(outputStream);
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
