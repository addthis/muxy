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
import java.io.OutputStream;

import com.addthis.basis.util.Bytes;

/* meta data for start/end of a stream */
public class MuxStream {

    protected int streamID;
    // so we can jump to the beginning of a stream on read
    protected int startFile;
    protected int startFileBlockOffset;
    // so we know when to stop looking for more blocks on read
    protected int endFile;
    protected int endFileBlockOffset;
    // so we know size
    protected long bytes;

    protected ReadMuxStreamDirectory streamDir;

    public MuxStream(ReadMuxStreamDirectory streamDir) {
        this.streamDir = streamDir;
    }

    public MuxStream(ReadMuxStreamDirectory streamDir, final InputStream in) throws IOException {
        this(streamDir);
        streamID = Bytes.readInt(in);
        startFile = Bytes.readInt(in);
        startFileBlockOffset = Bytes.readInt(in);
        endFile = Bytes.readInt(in);
        endFileBlockOffset = Bytes.readInt(in);
        bytes = Bytes.readLength(in);
    }

    protected void write(final OutputStream out) throws IOException {
        Bytes.writeInt(streamID, out);
        Bytes.writeInt(startFile, out);
        Bytes.writeInt(startFileBlockOffset, out);
        Bytes.writeInt(endFile, out);
        Bytes.writeInt(endFileBlockOffset, out);
        Bytes.writeLength(bytes, out);
    }

    public int getStreamID() {
        return streamID;
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
        return streamDir.readStream(this);
    }

    public OutputStream append(MuxStreamDirectory muxStreamDirectory) throws IOException {
        return muxStreamDirectory.appendStream(this);
    }

    @Override
    public String toString() {
        return "StreamMeta:" + streamID + "," + startFile + "#" + startFileBlockOffset + "," + endFile + "#" + endFileBlockOffset + "," + bytes;
    }
}
