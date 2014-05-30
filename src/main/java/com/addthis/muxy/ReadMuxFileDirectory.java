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

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import java.nio.file.Files;
import java.nio.file.Path;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Parameter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps MuxStreamDirectory to add file name to id mapping as
 * well as multi-part transparency.  Files are mapped to one or more
 * underlying streams (indexed by id).  This allows for efficient
 * clustering of file data that would otherwise be sparsely distributed
 * through the clustered block storage.
 */
public class ReadMuxFileDirectory {


    private static final Logger log = LoggerFactory.getLogger(ReadMuxFileDirectory.class);

    // trip-wire to prevent OOMs on too many records in directory
    protected static int MAX_RECORDS_READ = Parameter.intValue("muxy.file.max.records", 1000000);
    // return directory lists in sorted order
    protected static final boolean SORTED_DIR = Parameter.boolValue("muxy.dir.sorted", false);
    // optimize read only file reads (when hint is available)
    protected static final int DEFAULT_MAP_SIZE = Parameter.intValue("muxy.file.map.default.size", 257);

    protected final ReadMuxStreamDirectory streamMux;
    protected final MuxyEventListener eventListener;
    protected final Path streamDirectory;
    protected final Path fileMetaLog;
    protected final Path fileMetaConfig;
    protected final Map<String, ReadMuxFile> fileMap;

    protected int lastMapSize = DEFAULT_MAP_SIZE;

    /* returns true if this dir contains muxed files */
    public static boolean isMuxDir(Path dir) {
        Path conf = dir.resolve("mff.conf");
        Path data = dir.resolve("mff.data");
        return (Files.isRegularFile(conf) || Files.isRegularFile(data));
    }

    public ReadMuxFileDirectory(Path dir) throws Exception {
        this(dir, null);
    }

    public ReadMuxFileDirectory(Path dir, MuxyEventListener listener) throws Exception {
        try {
            this.eventListener = listener;
            this.streamMux = initMuxStreamDirectory(dir, listener);
            this.streamDirectory = dir.toRealPath();
            this.fileMetaLog = streamDirectory.resolve("mff.data");
            this.fileMetaConfig = streamDirectory.resolve("mff.conf");
            readConfig();
            this.fileMap = new HashMap<>(lastMapSize);
            readMetaLog();
        } catch (Exception ex) {
            System.err.println("MFM init error: dir=" + dir + " listener=" + listener);
            throw ex;
        }
    }

    protected ReadMuxStreamDirectory initMuxStreamDirectory(Path dir, MuxyEventListener listener) throws Exception {
        return new ReadMuxStreamDirectory(dir, listener);
    }

    public int getFileCount() {
        return fileMap.size();
    }

    public Path getDirectory() {
        return streamDirectory;
    }

    /* run once in constructor */
    protected void readConfig() throws IOException {
        if (Files.isRegularFile(fileMetaConfig)) {
            InputStream in = Files.newInputStream(fileMetaConfig);
            try {
                // Throw away write config settings
                Bytes.readInt(in);
                Bytes.readInt(in);
                if (in.available() > 0) {
                    lastMapSize = Bytes.readInt(in);
                }
            } catch (IOException ex) {
                log.error("corrupted conf {}", fileMetaConfig.toAbsolutePath());
            }
            in.close();
        }
    }

    public void publishEvent(MuxyFileEvent ID, Object target) {
        if (eventListener != null) {
            eventListener.fileEvent(ID, target);
        }
    }

    protected ReadMuxStreamDirectory getStreamManager() {

        return streamMux;
    }

    protected ReadMuxFile parseNextMuxFile(InputStream in) throws IOException {
        return new ReadMuxFile(in, this);
    }

    protected void readMetaLog() throws IOException {
        int recordsRead = 0;
        if (Files.isRegularFile(fileMetaLog)) {
            InputStream in = Files.newInputStream(fileMetaLog);
            while (true) {
                try {
                    ReadMuxFile meta = parseNextMuxFile(in);
                    if (recordsRead++ >= MAX_RECORDS_READ) {
                        throw new IOException("max records " + MAX_RECORDS_READ + " exceeded @ " + streamDirectory);
                    }
                    fileMap.put(meta.getName(), meta);
                } catch (EOFException ex) {
                    break;
                } catch (Exception ex) {
                    throw new IOException(ex);
                }
            }
            in.close();
        }
        publishEvent(MuxyFileEvent.LOG_READ, recordsRead);
    }

    public boolean exists(String fileName) {
        return fileMap.get(fileName) != null;
    }

    public ReadMuxFile openFile(String fileName, boolean create) throws IOException {
        ReadMuxFile fileMeta = fileMap.get(fileName);
        if (fileMeta == null) {
            throw new FileNotFoundException(fileName);
        }
        return fileMeta;
    }

    public Collection<ReadMuxFile> listFiles() throws IOException {
        if (SORTED_DIR) {
            SortedMap<String, ReadMuxFile> sorted = new TreeMap<>();
            for (ReadMuxFile meta : fileMap.values()) {
                sorted.put(meta.getName(), meta);
            }
            return sorted.values();
        } else {
            ArrayList<ReadMuxFile> list = new ArrayList<>(fileMap.size());
            for (ReadMuxFile meta : fileMap.values()) {
                list.add(meta);
            }
            return list;
        }
    }
}
