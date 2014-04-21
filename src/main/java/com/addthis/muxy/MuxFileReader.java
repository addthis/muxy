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

import java.io.IOException;
import java.io.InputStream;

import java.util.Iterator;
import java.util.zip.GZIPInputStream;

/*
 * this could be adapted to wait for more data when reading
 * at the end of a stream that's actively being written to.
 */
class MuxFileReader extends InputStream {

    private final Iterator<Integer> streams;
    private InputStream currentStream;
    private ReadMuxFileDirectory multiplexedFileManager;
    final boolean uncompress;

    MuxFileReader(ReadMuxFileDirectory multiplexedFileManager, Iterator<Integer> streams, boolean uncompress) {
        this.uncompress = uncompress;
        this.multiplexedFileManager = multiplexedFileManager;
        this.streams = streams;
    }

    MuxFileReader(ReadMuxFileDirectory multiplexedFileManager, Iterator<Integer> streams) {
        this.uncompress = false;
        this.multiplexedFileManager = multiplexedFileManager;
        this.streams = streams;
    }

    private boolean fill() throws IOException {
        while ((currentStream == null || currentStream.available() == 0)) {
            if (currentStream != null) {
                currentStream.close();
            }
            if (streams.hasNext()) {
                currentStream = multiplexedFileManager.getStreamManager().findStream(streams.next())
                        .read();
                if (uncompress) {
                    currentStream = new GZIPInputStream(currentStream);
                }
            } else {
                return false;
            }
        }
        return currentStream.available() > 0;
    }

    @Override
    public int read() throws IOException {
        return fill() ? currentStream.read() : -1;
    }

    @Override
    public void close() throws IOException {
        if (currentStream != null) {
            currentStream.close();
        }
    }

    @Override
    public int read(byte[] buf) throws IOException {
        return fill() ? currentStream.read(buf) : -1;
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        return fill() ? currentStream.read(buf, off, len) : -1;
    }
}
