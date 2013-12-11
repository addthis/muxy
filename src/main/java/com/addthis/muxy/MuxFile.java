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

import java.util.ArrayList;
import java.util.List;

import com.addthis.basis.util.JitterClock;

import com.google.common.primitives.Ints;

public class MuxFile extends ReadMuxFile {

    private final MuxFileDirectory writeDir;
    private final List<Integer> streamIDsList;

    public MuxFile(MuxFileDirectory writeDir) {
        super(writeDir);
        this.writeDir = writeDir;
        streamIDsList = new ArrayList<>(1);
    }

    public MuxFile(InputStream in, MuxFileDirectory writeDir) throws IOException {
        super(in, writeDir);
        this.writeDir = writeDir;
        streamIDsList = new ArrayList<>(Ints.asList(streamIDs));
    }

    public void sync() throws IOException {
        writeDir.writeStreamMux.writeStreamsToBlock();
    }

    public void setName(String newName) throws IOException {
        writeDir.acquireWritable();
        String oldName = fileName;
        synchronized (writeDir) {
            writeDir.fileMap.remove(oldName);
            if (newName != null) {
                if (writeDir.exists(newName)) {
                    MuxFile muxFile = (MuxFile) writeDir.fileMap.get(newName);
                    muxFile.delete();
                }
                writeDir.fileMap.put(newName, this);
            } else {
                /* delete associated stream ids */
                for (Integer streamID : getStreamIDs()) {
                    writeDir.writeStreamMux.deleteStream(streamID);
                }
            }
        }
        fileName = newName;
        if (newName != null) {
            writeDir.publishEvent(MuxyFileEvent.FILE_RENAME, new Object[]{oldName, newName});
        } else {
            writeDir.publishEvent(MuxyFileEvent.FILE_DELETE, oldName);
        }
    }

    public void delete() throws IOException {
        setName(null);
    }

    public OutputStream append() throws IOException {
        writeDir.publishEvent(MuxyFileEvent.FILE_APPEND, this);
        return writeDir.newStreamsOutput(this);
    }

    public OutputStream append(boolean compress) throws IOException {
        writeDir.publishEvent(MuxyFileEvent.FILE_APPEND, this);
        return writeDir.newStreamsOutput(this, compress);
    }

    public void addData(int bytes) throws IOException {
        writeDir.globalBytesWritten.addAndGet(bytes);
        length += bytes;
        lastModified = JitterClock.globalTime();
    }

    public void addStream(int streamID) throws IOException {
        streamIDsList.add(streamID);
    }

    @Override
    public List<Integer> getStreamIDs() {
        return streamIDsList;
    }
}
