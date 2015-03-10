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
import java.nio.file.Path;

import com.addthis.basis.util.LessBytes;
import com.addthis.basis.util.Parameter;

/* meta data for entire directory stored as binary in directory as mfs.conf */
class MuxDirectory {

    // Default block size in MB. default-default is 5
    static final int DEFAULT_BLOCK_SIZE = Parameter.intValue("muxy.default.block.size", 5) * 1024 * 1024;
    // Default file size in MB. default-default is 100
    static final int DEFAULT_FILE_SIZE = Parameter.intValue("muxy.default.file.size", 100) * 1024 * 1024;

    final AtomicInteger currentFile = new AtomicInteger(1);
    final AtomicInteger nextStreamID = new AtomicInteger(1);
    int maxBlockSize = DEFAULT_BLOCK_SIZE;
    int maxFileSize = DEFAULT_FILE_SIZE;
    int lockReleaseTimeout = 1000; // defaults to one second
    int streamMapSize = ReadMuxStreamDirectory.DEFAULT_MAP_SIZE; // see below: read, not written

    protected void read(Path dirMetaFile) throws IOException {
        if (Files.isRegularFile(dirMetaFile)) {
            try (InputStream in = Files.newInputStream(dirMetaFile)) {
                currentFile.set(LessBytes.readInt(in));
                nextStreamID.set(LessBytes.readInt(in));
                maxBlockSize = LessBytes.readInt(in);
                maxFileSize = LessBytes.readInt(in);
                lockReleaseTimeout = LessBytes.readInt(in);
            /* for backward compatibility. older confs do not contain this hint */
                if (in.available() > 0) {
                    streamMapSize = LessBytes.readInt(in);
                }
            }
        }
    }

    protected void write(Path dirMetaFile, int streamMapSize) throws IOException {
        try (OutputStream out = Files.newOutputStream(dirMetaFile)) {
            LessBytes.writeInt(currentFile.get(), out);
            LessBytes.writeInt(nextStreamID.get(), out);
            LessBytes.writeInt(maxBlockSize, out);
            LessBytes.writeInt(maxFileSize, out);
            LessBytes.writeInt(lockReleaseTimeout, out);
            // use as hint for allocation of map
            LessBytes.writeInt(streamMapSize, out);
        }
    }

    @Override
    public String toString() {
        return "DirMeta:" + currentFile + "," + nextStreamID + "," + maxBlockSize + "," + maxFileSize;
    }
}
