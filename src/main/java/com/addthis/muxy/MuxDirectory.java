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

import java.util.concurrent.atomic.AtomicInteger;

import java.nio.file.Files;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Parameter;

/* meta data for entire directory stored as binary in directory as mfs.conf */
class MuxDirectory {

    // Default block size in MB. default-default is 5
    protected static final int DEFAULT_BLOCK_SIZE = Parameter.intValue("muxy.default.block.size", 5) * 1024 * 1024;
    // Default file size in MB. default-default is 100
    protected static final int DEFAULT_FILE_SIZE = Parameter.intValue("muxy.default.file.size", 100) * 1024 * 1024;

    protected final AtomicInteger currentFile = new AtomicInteger(1);
    protected final AtomicInteger nextStreamID = new AtomicInteger(1);
    protected int maxBlockSize = DEFAULT_BLOCK_SIZE;
    protected int maxFileSize = DEFAULT_FILE_SIZE;
    protected int lockReleaseTimeout = 1000; // defaults to one second
    protected int streamMapSize = ReadMuxStreamDirectory.DEFAULT_MAP_SIZE; // see below: read, not written

    private final ReadMuxStreamDirectory muxStreamDirectory;

    public MuxDirectory(ReadMuxStreamDirectory muxStreamDirectory) {
        this.muxStreamDirectory = muxStreamDirectory;
    }

    public int getCurrentFile() {
        return currentFile.get();
    }

    public int getNextFile() throws IOException {
        try {
            return currentFile.incrementAndGet();
        } finally {
            write();
        }
    }

    public int getBlockTriggerSize() {
        return maxBlockSize;
    }

    public int getNextStreamID() throws IOException {
        try {
            return nextStreamID.incrementAndGet();
        } finally {
            write();
        }
    }

    public int getFileTriggerSize() {
        return maxFileSize;
    }

    public void setBlockTriggerSize(int size) throws IOException {
        maxBlockSize = size;
        write();
    }

    public void setFileTriggerSize(int size) throws IOException {
        maxFileSize = size;
        write();
    }

    public void setLockReleaseTimeout(int timeout) throws IOException {
        lockReleaseTimeout = timeout;
        write();
    }

    protected void read() throws IOException {
        if (Files.isRegularFile(muxStreamDirectory.dirMetaFile)) {
            try (InputStream in = Files.newInputStream(muxStreamDirectory.dirMetaFile)) {
                currentFile.set(Bytes.readInt(in));
                nextStreamID.set(Bytes.readInt(in));
                maxBlockSize = Bytes.readInt(in);
                maxFileSize = Bytes.readInt(in);
                lockReleaseTimeout = Bytes.readInt(in);
            /* for backward compatibility. older confs do not contain this hint */
                if (in.available() > 0) {
                    streamMapSize = Bytes.readInt(in);
                }
            }
        }
    }

    protected void write() throws IOException {
        try (OutputStream out = Files.newOutputStream(muxStreamDirectory.dirMetaFile)) {
            Bytes.writeInt(currentFile.get(), out);
            Bytes.writeInt(nextStreamID.get(), out);
            Bytes.writeInt(maxBlockSize, out);
            Bytes.writeInt(maxFileSize, out);
            Bytes.writeInt(lockReleaseTimeout, out);
            // use as hint for allocation of map
            Bytes.writeInt(muxStreamDirectory.streamDirectoryMap.size(), out);
        }
    }

    @Override
    public String toString() {
        return "DirMeta:" + currentFile + "," + nextStreamID + "," + maxBlockSize + "," + maxFileSize;
    }
}
