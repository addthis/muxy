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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * stream multiplexer. allows for a large number of append-only streams
 * to exist inside of a much smaller number of on-disk files. files consist
 * of a series of fast-skip blocks. each block contains a linked list of
 * bytes for 1 or more streams.
 */
public class MuxStreamDirectory extends ReadMuxStreamDirectory {

    private static final Logger log = LoggerFactory.getLogger(MuxStreamDirectory.class);

    private static final boolean DELETE_FREED_FILES = Boolean.getBoolean("muxy.delete.freed");

    /* openStreamWrites also acts as a barrier for all writing threads when global updates happen */
    protected final HashMap<Integer, StreamOut> openStreamWrites = new HashMap<>();
    protected final HashMap<Integer, StreamOut> pendingStreamCloses = new HashMap<>();
    protected final AtomicLong openWriteBytes = new AtomicLong(0);
    protected FileChannel openWriteFile;

    public MuxStreamDirectory(Path dir, MuxyStreamEventListener listener) throws Exception {
        super(dir, listener);
        this.deleteFreed = DELETE_FREED_FILES;
    }

    /**
     * automatically delete files no longer referenced by any streams.
     */
    public void setDeleteFreed(final boolean deleteFreed) {
        this.deleteFreed = deleteFreed;
    }

    public boolean isWritingComplete() {
        synchronized (openStreamWrites) {
            return openStreamWrites.isEmpty() && releaseComplete.get();
        }
    }

    /*
      * this method MUST be called when an application that performs writes is
      * done with this class.  it ensures that the file meta-data has been
      * properly compacted and written back out to disk.
      */
    public void waitForWriteClosure() {
        while (true) {
            synchronized (openStreamWrites) {
                if (isWritingComplete() || completeRelease()) {
                    return;
                }
            }
            try {
                Thread.sleep(100);
            } catch (Exception ex) {
                ex.printStackTrace();
                return;
            }
        }
    }

//  public InputStream readStream(MuxStream meta) throws IOException
//  {
//      synchronized (openStreamWrites)
//      {
//          return super.readStream(meta);
//      }
//  }

    /* acquire exclusive write lock for this directory */
    protected void acquireWritable() throws IOException {
        if (writeMutexFile == null) {
            writeMutexFile = FileChannel.open(streamDirectory.resolve("mfs.lock"), READ, WRITE, CREATE);
            writeMutexLock = writeMutexFile.lock();
            if (writeMutexLock.isShared()) {
                throw new IOException("unable to acquire exclusive lock for directory " + streamDirectory);
            }
            publishEvent(MuxyStreamEvent.WRITE_LOCK_ACQUIRED, writeMutexLock);
        }
        if (openWriteFile == null) {
            openWriteFile = FileChannel.open(getFileByID(streamDirectoryConfig.currentFile.intValue()), APPEND, CREATE);
            publishEvent(MuxyStreamEvent.BLOCK_FILE_WRITE_OPEN, streamDirectoryConfig.currentFile);
        }
    }

    /* write out a new meta log from in-memory map.
      * must be called with write lock held and in
      * a sync block on openStreamWrites
      */
    protected void compactMetaLog() throws IOException {
        Path tmpLog = Files.createTempFile(streamDirectory, dirDataFile.getFileName().toString(), ".tmp");
        OutputStream out = Files.newOutputStream(tmpLog);
        for (MuxStream meta : streamDirectoryMap.values()) {
            meta.write(out);
        }
        out.close();
        Files.move(tmpLog, dirDataFile, REPLACE_EXISTING);
        publishEvent(MuxyStreamEvent.LOG_COMPACT, streamDirectoryMap.size());
    }

    /* cause release to complete now if eligible
    *  thread safety - only called while synchronized on openStreamWrites */
    private boolean completeRelease() {
        try {
            /* all writes must be complete and release must not have run yet */
            if (!releaseComplete.get() && openStreamWrites.isEmpty()) {
                if (openWriteBytes.get() > 0) {
                    writeStreamsToBlock();
                }
                if (openWriteFile != null) {
                    openWriteFile.close();
                    openWriteFile = null;
                    publishEvent(MuxyStreamEvent.BLOCK_FILE_WRITE_CLOSE, streamDirectoryConfig.currentFile);
                }
                compactMetaLog();
                if (writeMutexLock != null) {
                    writeMutexLock.release();
                    writeMutexFile.close();
                    writeMutexFile = null;
                    publishEvent(MuxyStreamEvent.WRITE_LOCK_RELEASED, writeMutexLock);
                    writeMutexLock = null;
                }
                releaseComplete.set(true);
                return true;
            }
            return false;
        } catch (Exception ex) {
            /* this MFS is likely terminally f'd at this point */
            ex.printStackTrace();
            return false;
        }
    }

    public MuxStream createStream() throws IOException {
        MuxStream meta = new MuxStream(this);
        synchronized (openStreamWrites) {
            meta.streamID = reserveStreamID();
            streamDirectoryMap.put(meta.streamID, meta);
            publishEvent(MuxyStreamEvent.STREAM_CREATE, meta);
        }
        return meta;
    }

    @Override
    public Collection<MuxStream> listStreams() throws IOException {
        synchronized (openStreamWrites) {
            return super.listStreams();
        }
    }

    @Override
    public int size() {
        synchronized (openStreamWrites) {
            return super.size();
        }
    }

    @Override
    public MuxStream findStream(int streamID) throws IOException {
        synchronized (openStreamWrites) {
            return super.findStream(streamID);
        }
    }

    @Override
    public Collection<Path> getActiveFiles() throws IOException {
        synchronized (openStreamWrites) {
            return super.getActiveFiles();
        }
    }

    protected MuxStream deleteStream(final int streamID) throws IOException {
        synchronized (openStreamWrites) {
            final Set<Integer> usedSet = new HashSet<>();
            for (MuxStream metaAdd : streamDirectoryMap.values()) {
                usedSet.add(metaAdd.startFile);
                usedSet.add(metaAdd.endFile);
            }
            final MuxStream meta = streamDirectoryMap.remove(streamID);
            if (meta == null) {
                throw new IOException("No Such Stream ID " + streamID + " in " + streamDirectory);
            }
            publishEvent(MuxyStreamEvent.STREAM_DELETE, streamID);
            if (deleteFreed) {
                  /* scan chunk map and remove still-used files */
                for (MuxStream metaDel : streamDirectoryMap.values()) {
                    usedSet.remove(metaDel.startFile);
                    usedSet.remove(metaDel.endFile);
                }

                int currentFile = streamDirectoryConfig.currentFile.get();
                  /* whatever remains is unused */
                for (Integer delete : usedSet) {
                    Path file = getFileByID(delete);
                    if (!Files.deleteIfExists(file)) {
                        log.warn("Tried to delete os file, but it did not exist : {}", file);
                    }
                    log.debug("deleting stream {} freed {}", streamID, file);
                    publishEvent(MuxyStreamEvent.BLOCK_FILE_FREED, file);
                    if (delete == currentFile) //if we are deleting the current output file. Reopen it to recreate and init
                    {
                        openWriteFile = FileChannel.open(file, APPEND, CREATE);
                    }
                }
            }
            return meta;
        }
    }

    /* increment previous part record and start a new part */
    public OutputStream appendStream(MuxStream meta) throws IOException {
        synchronized (openStreamWrites) {
            acquireWritable();
            meta = findStream(meta.streamID);
            StreamOut streamOut = openStreamWrites.get(meta.streamID);
            if (streamOut == null) {
                streamOut = new StreamOut(meta);
                openStreamWrites.put(meta.streamID, streamOut);
            }
            publishEvent(MuxyStreamEvent.STREAM_APPEND, meta);
            releaseComplete.set(false);
            return streamOut.getWriter();
        }
    }

    /* really need to not be using byte array output streams */
    protected void trimOutputBuffers() {
        synchronized (openStreamWrites) {
            for (StreamOut out : openStreamWrites.values()) {
                synchronized (out) {
                    ByteArrayOutputStream buffer = out.output;
                    ByteArrayOutputStream trimBuffer = new ByteArrayOutputStream(buffer.size());
                    try {
                        buffer.writeTo(trimBuffer);
                    } catch (IOException e) {
                        // byte array output streams should not throw these
                        log.error("impossible exception", e);
                    }
                    out.output = trimBuffer;
                }
            }
        }
    }

    /* hold temp data for writing */
    private final class TempData {

        private final MuxStream meta;
        private final byte[] data;

        TempData(StreamOut stream) {
            meta = stream.meta;
            synchronized (stream) {
                data = stream.output.toByteArray();
                openWriteBytes.addAndGet(-data.length);
                stream.output.reset();
            }
        }
    }

    /* called when block threshold, close or timeout is hit */
    protected long writeStreamsToBlock() throws IOException {
        long writtenBytes = 0;
        synchronized (openStreamWrites) {
            /* yes, this could be optimized for concurrency by writing after lock is released, etc */
            if (openWriteBytes.get() == 0) {
                return 0;
            }
            List<TempData> streamsWithData = new ArrayList<>(openStreamWrites.size());
            for (StreamOut out : pendingStreamCloses.values()) {
                if (openStreamWrites.get(out.meta.streamID) != out && out.output.size() > 0) {
                    streamsWithData.add(new TempData(out));
                }
            }
            pendingStreamCloses.clear();
            for (StreamOut out : openStreamWrites.values()) {
                if (out.output.size() > 0) {
                    streamsWithData.add(new TempData(out));
                }
            }
            if (streamsWithData.size() == 0) {
                return 0;
            }
            for (TempData td : streamsWithData) {
                writtenBytes += td.data.length;
            }
            int streams = streamsWithData.size();
            publishEvent(MuxyStreamEvent.BLOCK_FILE_WRITE, streams);
            int currentFileOffset = (int) openWriteFile.size();
            ArrayList<ByteBuffer> srcs = new ArrayList<>(1 + streams);
            /* write out IDs in this block */
            ByteBuffer metaBuffer = ByteBuffer.allocate(2 + 4 * streams + 4 + 8 * streams);
            srcs.add(metaBuffer);
            metaBuffer.putShort((short) streams);
            int bodyOutputSize = 0;
            for (TempData out : streamsWithData) {
                metaBuffer.putInt(out.meta.streamID);
                /* (4) chunk body offset (4) chunk length (n) chunk bytes */
                bodyOutputSize += 8 + out.data.length;
            }
            /* write remainder size for rest of block data so that readers can skip if desired ID isn't present */
            metaBuffer.putInt(bodyOutputSize);
            /* write offsets and lengths for each stream id */
            int bodyOffset = streamsWithData.size() * 8;
            for (TempData out : streamsWithData) {
                metaBuffer.putInt(bodyOffset); //TODO - reconsider how frequently this shortcut is placed on disk
                metaBuffer.putInt(out.data.length);
                bodyOffset += out.data.length;
            }
            metaBuffer.flip();
            /* write bytes for each stream id */
            for (TempData out : streamsWithData) {
                srcs.add(ByteBuffer.wrap(out.data));
                out.meta.endFile = streamDirectoryConfig.getCurrentFile();
                out.meta.endFileBlockOffset = currentFileOffset;
                if (out.meta.startFile == 0) {
                    out.meta.startFile = streamDirectoryConfig.getCurrentFile();
                    out.meta.startFileBlockOffset = currentFileOffset;
                }
            }
            ByteBuffer[] srcsArray = srcs.toArray(new ByteBuffer[srcs.size()]);
            while (openWriteFile.write(srcsArray) > 0) {
                ;
            }
            /* check for rolling current file on size threshold */
            if (openWriteFile.size() > streamDirectoryConfig.maxFileSize) {
                openWriteFile.close();
                openWriteFile = FileChannel.open(getFileByID(streamDirectoryConfig.getNextFile()), APPEND, CREATE);
                publishEvent(MuxyStreamEvent.BLOCK_FILE_WRITE_ROLL, streamDirectoryConfig.currentFile);
            }
        }
        return writtenBytes;
    }

    /* wrapper for writing into chunks */
    protected final class StreamOut {

        protected final MuxStream meta;
        protected ByteArrayOutputStream output;
        protected final AtomicInteger writers = new AtomicInteger(0);

        protected StreamOut(final MuxStream meta) {
            this.meta = meta;
            this.output = new ByteArrayOutputStream();
        }

        public OutputStream getWriter() {
            writers.incrementAndGet();
            return new StreamOutWriter(this);
        }

        protected void write(final byte b[], final int off, final int len) throws IOException {
            // burp out a block if we hit a threshold
            if (openWriteBytes.get() >= streamDirectoryConfig.maxBlockSize) {
                synchronized (openStreamWrites) {
                    if (openWriteBytes.get() >= streamDirectoryConfig.maxBlockSize) {
                        writeStreamsToBlock();
                    }
                }
            }
            synchronized (this) {
                output.write(b, off, len);
                openWriteBytes.addAndGet(len);
                meta.bytes += len;
            }
        }

        protected void close() throws IOException {
            synchronized (openStreamWrites) {
                publishEvent(MuxyStreamEvent.STREAM_CLOSE, meta);
                if (writers.decrementAndGet() == 0) {
                    publishEvent(MuxyStreamEvent.STREAM_CLOSED_ALL, meta);
                    openStreamWrites.remove(meta.streamID);
                    if (openStreamWrites.size() == 0) {
                        closeTime.set(System.currentTimeMillis());
                        publishEvent(MuxyStreamEvent.CLOSED_ALL_STREAM_WRITERS, meta);
                    }
                    StreamOut existingPend = pendingStreamCloses.get(meta.streamID);
                    if (existingPend != null && existingPend != this) {
                        synchronized (this) {
                            output.writeTo(existingPend.output);
                        }
                    } else {
                        pendingStreamCloses.put(meta.streamID, this);
                    }
                }
            }
        }
    }

    /* for tracking # of writers per output stream */
    protected final class StreamOutWriter extends OutputStream {

        protected StreamOut out;

        protected StreamOutWriter(StreamOut out) {
            this.out = out;
        }

        @Override
        protected void finalize() {
            try {
                close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        @Override
        public void write(int arg0) throws IOException {
            write(new byte[]{(byte) arg0}, 0, 1);
        }

        @Override
        public void write(byte b[], int off, int len) throws IOException {
            out.write(b, off, len);
        }

        @Override
        public void close() throws IOException {
            if (out != null) {
                out.close();
                out = null;
            }
        }
    }

}
