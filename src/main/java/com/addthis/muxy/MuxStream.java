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
import java.io.OutputStream;

import java.nio.file.Path;

import com.addthis.basis.util.LessBytes;

import com.google.common.base.MoreObjects;

/* meta data for start/end of a stream */
public class MuxStream {

    protected final int streamId;
    // so we can jump to the beginning of a stream on read
    protected int startFile;
    protected int startFileBlockOffset;
    // so we know when to stop looking for more blocks on read
    protected int endFile;
    protected int endFileBlockOffset;
    // so we know size
    protected long bytes;

    @Nonnull  private final Path streamDirectory;
    @Nullable private final MuxyEventListener eventListener;

    public MuxStream(ReadMuxStreamDirectory streamDir, int streamId) {
        this.streamDirectory = streamDir.streamDirectory;
        this.eventListener = streamDir.eventListener;
        this.streamId = streamId;
    }

    public MuxStream(ReadMuxStreamDirectory streamDir, InputStream in) throws IOException {
        this.streamDirectory = streamDir.streamDirectory;
        this.eventListener = streamDir.eventListener;
        this.streamId = LessBytes.readInt(in);
        this.startFile = LessBytes.readInt(in);
        this.startFileBlockOffset = LessBytes.readInt(in);
        this.endFile = LessBytes.readInt(in);
        this.endFileBlockOffset = LessBytes.readInt(in);
        this.bytes = LessBytes.readLength(in);
    }

    protected void write(final OutputStream out) throws IOException {
        LessBytes.writeInt(streamId, out);
        LessBytes.writeInt(startFile, out);
        LessBytes.writeInt(startFileBlockOffset, out);
        LessBytes.writeInt(endFile, out);
        LessBytes.writeInt(endFileBlockOffset, out);
        LessBytes.writeLength(bytes, out);
    }

    public int getStreamId() {
        return streamId;
    }

    public long getStreamBytes() {
        return bytes;
    }

    public int getStartFile() {
        return startFile;
    }

    public int getEndFile() {
        return endFile;
    }

    public int getStartBlockOffset() {
        return startFileBlockOffset;
    }

    public int getEndBlockOffset() {
        return endFileBlockOffset;
    }

    public InputStream read() throws IOException {
        if (startFile == 0) {
            throw new IOException("uninitialized stream");
        }
        return new StreamIn(this, streamDirectory, eventListener);
    }

    public OutputStream append(MuxStreamDirectory muxStreamDirectory) throws IOException {
        return muxStreamDirectory.appendStream(this);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                      .add("streamId", streamId)
                      .add("bytes", bytes)
                      .add("startFile", startFile)
                      .add("startFileBlockOffset", startFileBlockOffset)
                      .add("endFile", endFile)
                      .add("endFileBlockOffset", endFileBlockOffset)
                      .toString();
    }
}
