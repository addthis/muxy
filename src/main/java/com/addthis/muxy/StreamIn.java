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
package com.addthis.muxy;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* this is much trickier */
final class StreamIn extends InputStream {
    private static final Logger log = LoggerFactory.getLogger(StreamIn.class);

    final MuxStream meta;
    FileChannel input;
    int currentFile;
    int currentRemain;
    long nextBlockPosition;

    @Nonnull  private final Path streamDirectory;
    @Nullable private final MuxyEventListener eventListener;

    StreamIn(MuxStream meta,
             @Nonnull Path streamDirectory,
             @Nullable MuxyEventListener eventListener) throws IOException {
        publishEvent(MuxyStreamEvent.STREAM_READ, meta);
        this.meta = meta;
        this.streamDirectory = streamDirectory;
        this.eventListener = eventListener;
        this.currentFile = meta.startFile;
        input = FileChannel.open(ReadMuxStreamDirectory.getFileByID(streamDirectory, meta.startFile));
        input.position(meta.startFileBlockOffset);
        publishEvent(MuxyStreamEvent.BLOCK_FILE_READ_OPEN, currentFile);
    }

    private void publishEvent(MuxyStreamEvent ID, Object target) {
        if (eventListener != null) {
            eventListener.streamEvent(ID, target);
        }
    }

    @Override
    public void close() throws IOException {
        publishEvent(MuxyStreamEvent.BLOCK_FILE_READ_CLOSE, currentFile);
        input.close();
    }

    @Override
    public int available() throws IOException {
        return fill() ? currentRemain : 0;
    }

    /* assumes file pointer is at the beginning of a valid block */
    /* return true if more data is available */
    protected boolean fill() throws IOException {
        while (currentRemain == 0 && currentFile <= meta.endFile) {
            if (currentFile == meta.endFile && input.position() > meta.endFileBlockOffset) {
                return false;
            }
            if (nextBlockPosition != 0) {
                input.position(nextBlockPosition);
            }
            if (input.position() >= input.size()) {
                input.close();
                publishEvent(MuxyStreamEvent.BLOCK_FILE_READ_CLOSE, currentFile);
                Path nextFile = ReadMuxStreamDirectory.getFileByID(streamDirectory, ++currentFile);
                if (!Files.exists(nextFile)) {
                    log.warn("terminating stream on missing: {}", nextFile);
                    return false;
                }
                input = FileChannel.open(nextFile);
                nextBlockPosition = 0;
                publishEvent(MuxyStreamEvent.BLOCK_FILE_READ_OPEN, currentFile);
            }
            // find next block that has this stream id
            ByteBuffer shortBuffer = ByteBuffer.allocate(2);
            while (input.read(shortBuffer) > 0) {
                ;
            }
            shortBuffer.flip();
            int countIDs = shortBuffer.getShort();

            int bufferSize = Math.min(1024, countIDs);
            ByteBuffer buffer = ByteBuffer.allocateDirect(4 * bufferSize);
            int[] streams = new int[bufferSize];
            int offset = 0;
            int offsetFound = 0;
            while (countIDs > 0) {
                buffer.clear();
                if (countIDs < bufferSize) {
                    buffer.limit(countIDs * 4);
                }
                while (input.read(buffer) > 0) {
                    ;
                }
                buffer.flip();
                IntBuffer ibuffer = buffer.asIntBuffer();
                int ibuffRemain = Math.min(ibuffer.remaining(), countIDs);
                ibuffer.get(streams, 0, ibuffRemain);
                for (int i = 0; i < ibuffRemain; i++) {
                    if (streams[i] == meta.streamId) {
                        offsetFound = offset + i + 1;
                        //TODO break and seek ahead based on countIDs
                    }
                }
                countIDs -= ibuffRemain;
                offset += ibuffRemain;
            }
            buffer = ByteBuffer.allocate(4);
            while (input.read(buffer) > 0) {
                ;
            }
            buffer.flip();
            int bodySize = buffer.getInt();
            long currentPosition = input.position();
            nextBlockPosition = currentPosition + bodySize;
            if (offsetFound == 0) {
                input.position(nextBlockPosition);
                continue;
            } else {
                input.position(currentPosition + 8 * (offsetFound - 1));
            }
            ByteBuffer chunkBuffer = ByteBuffer.allocate(8);
            while (input.read(chunkBuffer) > 0) {
                ;
            }
            chunkBuffer.flip();
            int chunkBodyOffset = chunkBuffer.getInt();
            int chunkBodyLength = chunkBuffer.getInt();
            input.position(currentPosition + chunkBodyOffset);
            currentRemain = chunkBodyLength;
        }
        return currentRemain > 0;
    }

    ByteBuffer singleByte;

    @Override
    public int read() throws IOException {
        if (fill()) {
            if (singleByte == null) {
                singleByte = ByteBuffer.allocate(1);
            }
            singleByte.clear();
            int read = input.read(singleByte);
            if (read >= 0) {
                currentRemain--;
            }
            singleByte.flip();
            return singleByte.get() & 0xff;
        }
        return -1;
    }

    @Override
    public int read(final byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        if (!fill()) {
            return -1;
        }
        final int read = input.read(ByteBuffer.wrap(b, off, Math.min(len, currentRemain)));
        if (read > 0) {
            currentRemain -= read;
        }
        return read;
    }
}
