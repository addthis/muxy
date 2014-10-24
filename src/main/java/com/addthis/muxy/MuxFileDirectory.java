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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Parameter;

import com.google.common.base.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Wraps MuxStreamDirectory to add file name to id mapping as
 * well as multi-part transparency.  Files are mapped to one or more
 * underlying streams (indexed by id).  This allows for efficient
 * clustering of file data that would otherwise be sparsely distributed
 * through the clustered block storage.
 */
public class MuxFileDirectory extends ReadMuxFileDirectory {

    private static final Logger log = LoggerFactory.getLogger(MuxFileDirectory.class);

    // max size for a sub-stream in KB. Mostly for testing / debugging
    static final int STREAM_MAXSIZE = Parameter.intValue("STREAM_MAXSIZE", 0) * 1024;
    // write thrashing threshold
    static int WRITE_THRASHOLD = Parameter.intValue("muxy.write.thrashold", 10) * 1024 * 1024;
    // lazy log close delay (defaults to 500ms)
    static int LAZY_LOG_CLOSE = Parameter.intValue("muxy.close.wait", 1000);
    // max time to block waiting for all writers to exit (in seconds)
    static int EXIT_CLOSURE_TIMEOUT = Parameter.intValue("muxy.exit.timeout", 300) * 1000;
    // force closure after exit timeout
    static boolean EXIT_CLOSURE_TIMEOUT_FORCE = Parameter.boolValue("muxy.exit.timeout.force", true);
    // time to wait before attempting to write out dir map after last close
    static int WRITE_CLOSE_GRACE_TIME = Parameter.intValue("muxy.file.write.close", 10000);

    private final ConcurrentMap<StreamsWriter, StreamsWriter> openFileWrites = new ConcurrentHashMap<>();
    private final AtomicBoolean releaseComplete = new AtomicBoolean(true);
    private final AtomicLong closeTime = new AtomicLong(0);
    protected final MuxStreamDirectory writeStreamMux;
    protected final MuxFileDirectoryCacheInstance cacheInstance;

    final AtomicLong globalBytesWritten = new AtomicLong(0);

    private FileChannel writeMutexFile;
    private FileLock writeMutexLock;

    public MuxFileDirectory(Path dir, MuxyEventListener listener) throws Exception {
        this(dir, listener, MuxFileDirectoryCache.DEFAULT);
    }

    public MuxFileDirectory(Path dir, MuxyEventListener listener, MuxFileDirectoryCacheInstance cacheInstance) throws Exception {
        super(dir, listener);
        this.cacheInstance = cacheInstance;
        writeStreamMux = (MuxStreamDirectory) streamMux;
    }

    @Override
    protected MuxStreamDirectory initMuxStreamDirectory(Path dir, MuxyEventListener listener) throws Exception {
        return new MuxStreamDirectory(dir, listener);
    }

    @Override
    public synchronized int getFileCount() {
        return super.getFileCount();
    }

    @Override
    protected WritableMuxFile parseNextMuxFile(InputStream in) throws IOException {
        return new WritableMuxFile(in, this);
    }

    @Override
    public synchronized Collection<MuxFile> listFiles() throws IOException {
        return super.listFiles();
    }

    public void setDeleteFreed(final boolean deleteFreed) {
        writeStreamMux.setDeleteFreed(deleteFreed);
    }

    public void setWriteThrashold(int threshold) throws IOException {
        WRITE_THRASHOLD = threshold;
        writeConfig();
    }

    public void setLazyLogClose(int timeoutMS) throws IOException {
        LAZY_LOG_CLOSE = timeoutMS;
        writeConfig();
    }

    /* pass through config */
    public void setMaxBlockSize(int size) throws IOException {
        streamMux.setMaxBlockSize(size);
    }

    /* pass through config */
    public void setMaxFileSize(int size) throws IOException {
        streamMux.setMaxFileSize(size);
    }

    public boolean isWritingComplete() {
        synchronized (openFileWrites) {
            return openFileWrites.isEmpty() && releaseComplete.get();
        }
    }

    public void waitForWriteClosure() {
        waitForWriteClosure(EXIT_CLOSURE_TIMEOUT);
    }

    public boolean waitForWriteClosure(final int waitTime) {
        long maxWait = JitterClock.globalTime() + waitTime;
        while (true) {
            boolean closed = false;
            synchronized (openFileWrites) {
                closed = isWritingComplete() || completeRelease();
            }
            if (closed) {
                writeStreamMux.waitForWriteClosure();
                return true;
            }
            if (waitTime == 0) {
                return false;
            }
            if (JitterClock.globalTime() > maxWait) {
                log.warn("waitForWriteClosure() timeout={} openFileWrites={} released={} dir={}",
                         waitTime, openFileWrites.size(), releaseComplete, streamDirectory);
                if (EXIT_CLOSURE_TIMEOUT_FORCE) {
                    synchronized (openFileWrites) {
                        for (StreamsWriter writer : openFileWrites.values()) {
                            try {
                                log.warn(" force closing {}", writer.meta.fileName);
                                writer.close();
                            } catch (Exception ex) {
                                log.error("error force closing", ex);
                            }
                        }
                        completeRelease();
                    }
                }
                writeStreamMux.waitForWriteClosure();
                return false;
            }
            try {
                Thread.sleep(10);
            } catch (Exception ex) {
                ex.printStackTrace();
                return false;
            }
        }
    }

    /* called for each property change */
    private void writeConfig() throws IOException {
        OutputStream out = Files.newOutputStream(fileMetaConfig);
        Bytes.writeInt(WRITE_THRASHOLD, out);
        Bytes.writeInt(LAZY_LOG_CLOSE, out);
        Bytes.writeInt(fileMap.size(), out);
        out.close();
    }

    /* acquire exclusive write write lock for this directory */
    void acquireWritable() throws IOException {
        synchronized (openFileWrites) {
            if (writeMutexFile == null) {
                FileChannel tmp = FileChannel.open(streamDirectory.resolve("mff.lock"), READ, WRITE, CREATE);
                writeMutexLock = tmp.lock();
                if (writeMutexLock == null || writeMutexLock.isShared()) {
                    throw new IOException("unable to acquire exclusive writeLock for directory " + streamDirectory);
                }
                writeMutexFile = tmp;
                publishEvent(MuxyFileEvent.WRITE_LOCK_ACQUIRED, writeMutexLock);
            }
            releaseComplete.set(false);
        }
    }

    /* cause release to complete now if eligible.
     * must be called with sync on openFileWrites! */
    private boolean completeRelease() {
        try {
            /* all writes must be complete and release must not have run yet */
            if (!releaseComplete.get() && openFileWrites.isEmpty()) {
                compactMetaLog();
                if (writeMutexLock != null) {
                    writeMutexLock.release();
                    writeMutexFile.close();
                    writeMutexFile = null;
                    publishEvent(MuxyFileEvent.WRITE_LOCK_RELEASED, writeMutexLock);
                    writeMutexLock = null;
                }
                releaseComplete.set(true);
            }
        } catch (Exception ex) {
            /* this MFF is likely terminally f'd at this point */
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
        return releaseComplete.get();
    }

    @Override
    protected MuxStreamDirectory getStreamManager() {
        return writeStreamMux;
    }

    private synchronized void compactMetaLog() throws IOException {
        Path tmpLog = Files.createTempFile(streamDirectory, fileMetaLog.getFileName().toString(), ".tmp");
        OutputStream out = Files.newOutputStream(tmpLog);
        for (MuxFile meta : fileMap.values()) {
            meta.writeRecord(out);
        }
        out.close();
        Files.move(tmpLog, fileMetaLog, REPLACE_EXISTING);
        publishEvent(MuxyFileEvent.LOG_COMPACT, fileMap.size());
    }

    @Override
    // Override adds the synchronized keyword
    public synchronized boolean exists(String fileName) {
        return super.exists(fileName);
    }

    @Override
    public synchronized WritableMuxFile openFile(String fileName, boolean create) throws IOException {
        WritableMuxFile muxFile = (WritableMuxFile) fileMap.get(fileName);
        if (muxFile == null && create) {
            muxFile = new WritableMuxFile(this);
            muxFile.fileId = writeStreamMux.reserveStreamID();
            muxFile.fileName = fileName;
            fileMap.put(fileName, muxFile);
            publishEvent(MuxyFileEvent.FILE_CREATE, muxFile);
        }
        if (muxFile == null) {
            throw new FileNotFoundException(fileName);
        }
        return muxFile;
    }

    /**
     * iterate of all files and re-write them sequentially into new file space
     */
    protected synchronized void defrag() throws IOException {
        int block_size = Parameter.intValue("block-size", 1) * 1024 * 1024;
        int file_size = Parameter.intValue("file-size", 100) * 1024 * 1024;
        boolean decompress = Parameter.boolValue("decompress", false);
        boolean recompress = Parameter.boolValue("recompress", false);
        streamMux.setMaxBlockSize(block_size);
        streamMux.setMaxFileSize(file_size);

        int firstNew = writeStreamMux.bumpCurrentFile();
        System.out.println("defragging: " + fileMap.size() + " files into chunks starting with " + MuxStreamDirectory.formatFileName(firstNew));
        for (MuxFile oldFile : new ArrayList<>(fileMap.values())) {
            System.out.print(oldFile.getName() + ", ");
            WritableMuxFile newFile = openFile(UUID.randomUUID().toString(), true);
            byte[] buf = new byte[4096];
            int read = 0;
            InputStream in = oldFile.read(0, decompress);
            OutputStream out = newFile.append(recompress);
            while ((read = in.read(buf)) >= 0) {
                out.write(buf, 0, read);
            }
            out.close();
            in.close();
            newFile.lastModified = oldFile.getLastModified();
            newFile.setName(oldFile.getName());
            writeStreamMux.writeStreamsToBlock();
        }
        while (firstNew > 1) {
            Files.deleteIfExists(streamDirectory.resolve(MuxStreamDirectory.formatFileName(--firstNew)));
        }
        waitForWriteClosure();
        System.out.println();
    }

    public OutputStream newStreamsOutput(WritableMuxFile muxFile) throws IOException {
        return new StreamsWriter(muxFile);
    }

    public OutputStream newStreamsOutput(WritableMuxFile muxFile, boolean compress) throws IOException {
        return new StreamsWriter(muxFile, compress);
    }

    /** */
    private class StreamsWriter extends OutputStream {

        private final WritableMuxFile meta;
        private OutputStream currentStream;
        private long lastGlobalBytes;
        private long bytesWritten;
        private final boolean compress;

        StreamsWriter(WritableMuxFile meta, boolean compress) throws IOException {
            acquireWritable();
            this.meta = meta;
            this.lastGlobalBytes = globalBytesWritten.get();
            this.compress = compress;
        }

        StreamsWriter(WritableMuxFile meta) throws IOException {
            this(meta, false);
        }

        @Override
        protected void finalize() {
            try {
                if (maybeClose()) {
                    log.error("Finalize method had to close StreamWriter: {}", this);
                }
            } catch (Exception ex) {
                log.error("Finalize method encountered an exception while trying to close StreamWriter: {}", this);
            }
        }

        private void checkStreamForWrite() throws IOException {
            /**
             * only increment on first write to prevent close from
             * blocking indefinitely on openFileWrites accounting
             */
            if (bytesWritten == 0) {
                synchronized (openFileWrites) {
                    openFileWrites.put(this, this);
                }
            }
            if (STREAM_MAXSIZE > 0 && bytesWritten > STREAM_MAXSIZE) {
                closeCurrentStream();
            }
            /**
             * if more than 10MB have been written globally since we last wrote, create another stream
             * rather than have our stream cause excessive write (and thus read) fragmentation.
             */
//          else if (bytesWritten > 0 && globalBytesWritten.get() - lastGlobalBytes > WRITE_THRASHOLD)
//          {
//              publishEvent(MuxyFileEvent.TRIGGERED_WRITE_THRESHOLD, meta);
//              closeCurrentStream();
//          }
            if (currentStream == null) {
                MuxStream streamMeta = writeStreamMux.createStream();
                meta.addStream(streamMeta);
                currentStream = streamMeta.append(writeStreamMux);
                if (compress) {
                    currentStream = new GZIPOutputStream(currentStream);
                }
                bytesWritten = 0;
            }
            lastGlobalBytes = globalBytesWritten.get();
        }

        @Override
        public synchronized void write(int i) throws IOException {
            checkStreamForWrite();
            currentStream.write(i);
            bytesWritten++;
            meta.addData(1);
        }

        @Override
        public void write(byte[] buf) throws IOException {
            write(buf, 0, buf.length);
        }

        @Override
        public synchronized void write(byte[] buf, int off, int len) throws IOException {
            checkStreamForWrite();
            currentStream.write(buf, off, len);
            bytesWritten += len;
            meta.addData(len);
        }

        private void closeCurrentStream() throws IOException {
            if (currentStream != null) {
                currentStream.close();
                currentStream = null;
            }
        }

        private synchronized boolean maybeClose() throws IOException {
            if (currentStream != null) {
                closeCurrentStream();
                publishEvent(MuxyFileEvent.FILE_CLOSE, meta);
                boolean deleted = false;
                synchronized (openFileWrites) {
                    deleted = openFileWrites.remove(this) != null;
                }
                if (deleted) {
                    publishEvent(MuxyFileEvent.CLOSED_ALL_FILE_WRITERS, meta);
                }
                return true;
            }
            return false;
        }

        @Override
        public void close() throws IOException {
            maybeClose();
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("meta", meta)
                    .add("currentStream", currentStream)
                    .add("lastGlobalBytes", lastGlobalBytes)
                    .add("bytesWritten", bytesWritten)
                    .add("compress", compress)
                    .toString();
        }
    }

}
