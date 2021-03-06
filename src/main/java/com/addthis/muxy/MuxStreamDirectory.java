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

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.OutputStream;

import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
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
    // 511 is just under 512, and thus will avoid a netty bug causing double allocation for the default size.
    // I tried to make the default even just slightly higher previously, but discovered that the worst case
    // current downstream use has far more tiny directories and streams than I imagined. So for now at least
    // we will use a small default and let the more common large-buffer case pay the price.
    private static final int BUFFER_MIN_SIZE = Integer.getInteger("muxy.buffer.min", 511);
    private static final String DATA_FILE_PERMISSIONS = System.getProperty("muxy.data.file.permissions", "rw-rw-r--");
    public static final FileAttribute<?> DATA_FILE_ATTRIBUTES = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString(DATA_FILE_PERMISSIONS));

    /* openWritesLock also acts as a barrier for all writing threads when global updates happen */
    protected final ReentrantLock openWritesLock = new ReentrantLock();
    @GuardedBy("openWritesLock") protected final Map<Integer, StreamOut> openStreamWrites = new HashMap<>();

    protected final Map<Integer, StreamOut> pendingStreamCloses = new HashMap<>();
    protected final AtomicLong openWriteBytes = new AtomicLong(0);
    protected FileChannel openWriteFile;

    public MuxStreamDirectory(Path dir, MuxyEventListener listener) throws Exception {
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
        openWritesLock.lock();
        try {
            return openStreamWrites.isEmpty() && releaseComplete.get();
        } finally {
            openWritesLock.unlock();
        }
    }

    protected int reserveStreamID() throws IOException {
        int streamId = streamDirectoryConfig.nextStreamID.incrementAndGet();
        releaseComplete.set(false);
        return streamId;
    }

    /* force new "current" file -- used in defrag operations */
    protected int bumpCurrentFile() throws IOException {
        int fileId = streamDirectoryConfig.currentFile.incrementAndGet();
        releaseComplete.set(false);
        return fileId;
    }

    public void setMaxBlockSize(int size) throws IOException {
        streamDirectoryConfig.maxBlockSize = size;
        releaseComplete.set(false);
    }

    public void setMaxFileSize(int size) throws IOException {
        streamDirectoryConfig.maxFileSize = size;
        releaseComplete.set(false);
    }

    /**
     * this method MUST be called when an application that performs writes is
     * done with this class.  it ensures that the file meta-data has been
     * properly compacted and written back out to disk.
     */
    public void waitForWriteClosure() {
        while (true) {
            if (openWritesLock.tryLock()) {
                try {
                    if (isWritingComplete() || completeRelease()) {
                        return;
                    }
                } finally {
                    openWritesLock.unlock();
                }
            }
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }
    }

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

    /**
     * write out a new meta log from in-memory map.
     * must be called with write lock held and in a sync block on openStreamWrites
     */
    protected void compactMetaLog() throws IOException {
        Path tmpLog = Files.createTempFile(streamDirectory, dirDataFile.getFileName().toString(), ".tmp", DATA_FILE_ATTRIBUTES);
        OutputStream out = Files.newOutputStream(tmpLog);
        for (MuxStream meta : streamDirectoryMap.values()) {
            meta.write(out);
        }
        out.close();
        Files.move(tmpLog, dirDataFile, REPLACE_EXISTING);
        publishEvent(MuxyStreamEvent.LOG_COMPACT, streamDirectoryMap.size());
    }

    /**
     * cause release to complete now if eligible
     * thread safety - only called while synchronized on openStreamWrites
     */
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
                streamDirectoryConfig.write(dirMetaFile, streamDirectoryMap.size());
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
        MuxStream meta;
        openWritesLock.lock();
        try {
            int newMetaId = reserveStreamID();
            meta = new MuxStream(this, newMetaId);
            streamDirectoryMap.put(meta.streamId, meta);
        } finally {
            openWritesLock.unlock();
        }
        publishEvent(MuxyStreamEvent.STREAM_CREATE, meta);
        eventListener.reportStreams(1);
        return meta;
    }

    @Override
    public Collection<MuxStream> listStreams() throws IOException {
        openWritesLock.lock();
        try {
            return super.listStreams();
        } finally {
            openWritesLock.unlock();
        }
    }

    @Override
    public int size() {
        openWritesLock.lock();
        try {
            return super.size();
        } finally {
            openWritesLock.unlock();
        }
    }

    @Override
    public MuxStream findStream(int streamID) throws IOException {
        openWritesLock.lock();
        try {
            return super.findStream(streamID);
        } finally {
            openWritesLock.unlock();
        }
    }

    @Override
    public Collection<Path> getActiveFiles() throws IOException {
        openWritesLock.lock();
        try {
            return super.getActiveFiles();
        } finally {
            openWritesLock.unlock();
        }
    }

    protected MuxStream deleteStream(final int streamID) throws IOException {
        openWritesLock.lock();
        try {
            MuxStream deletedMeta = streamDirectoryMap.remove(streamID);
            if (deletedMeta == null) {
                throw new IOException("No Such Stream ID " + streamID + " in " + streamDirectory);
            }
            publishEvent(MuxyStreamEvent.STREAM_DELETE, streamID);
            if (deleteFreed) {
                int currentFileId = streamDirectoryConfig.currentFile.get();
                int startFileId   = startFile;
                int[] fileSpansPerStart = new int[currentFileId - startFileId + 1];
                log.trace("current {} start {} length {}", currentFileId, startFileId,
                          (currentFileId - startFileId) + 1);
                for (MuxStream meta : streamDirectoryMap.values()) {
                    fileSpansPerStart[meta.startFile - startFileId] =
                            Math.max(fileSpansPerStart[meta.startFile - startFileId], meta.endFile);
                }
                int usedFilesLookahead = -1;
                for (int i = 0; i < fileSpansPerStart.length; i++) {
                    int length = fileSpansPerStart[i] - i;
                    usedFilesLookahead = Math.max(length, usedFilesLookahead);
                    usedFilesLookahead -= 1;
                    if (usedFilesLookahead < 0) {
                        // fileId is unused
                        int fileId = i + startFileId;
                        Path file = getFileByID(fileId);
                        if (Files.deleteIfExists(file)) {
                            log.debug("Deleted freed file {}", file);
                            publishEvent(MuxyStreamEvent.BLOCK_FILE_FREED, file);
                            //  if we are deleting the current output file. Reopen it to recreate and init
                            if (fileId == currentFileId) {
                                openWriteFile = FileChannel.open(file, APPEND, CREATE);
                            }
                        }
                        if ((fileId == startFile) && (fileId != currentFileId)) {
                            startFile += 1;
                        }
                    }
                }
            }
            return deletedMeta;
        } finally {
            openWritesLock.unlock();
        }
    }

    /* increment previous part record and start a new part */
    public OutputStream appendStream(MuxStream meta) throws IOException {
        openWritesLock.lock();
        try {
            acquireWritable();
            meta = findStream(meta.streamId);
            StreamOut streamOut = openStreamWrites.get(meta.streamId);
            if (streamOut == null) {
                streamOut = new StreamOut(meta);
                openStreamWrites.put(meta.streamId, streamOut);
            }
            publishEvent(MuxyStreamEvent.STREAM_APPEND, meta);
            releaseComplete.set(false);
            return streamOut.getWriter();
        } finally {
            openWritesLock.unlock();
        }
    }

    /** Trims memory overhead if the openWritesLock is immediately available. */
    protected boolean maybeTrimOutputBuffers() {
        if (openWritesLock.tryLock()) {
            try {
                for (StreamOut out : openStreamWrites.values()) {
                    synchronized (out) {
                        if (out.outputBuffer.readableBytes() == 0) {
                            out.outputBuffer.capacity(0);
                        } else {
                            out.outputBuffer.discardReadBytes();
                            if (out.outputBuffer instanceof CompositeByteBuf) {
                                ((CompositeByteBuf) out.outputBuffer).consolidate();
                            }
                        }
                    }
                }
            } finally {
                openWritesLock.unlock();
            }
            return true;
        }
        return false;
    }

    /* hold temp data for writing */
    private final class TempData {

        private final MuxStream meta;
        private final ByteBuf data;
        private final StreamOut stream;
        private final int snapshotLength;

        TempData(StreamOut stream) {
            this.stream = stream;
            meta = stream.meta;
            data = stream.outputBuffer;
            snapshotLength = data.readableBytes();
        }
    }

    /* called when block threshold, close or timeout is hit */
    protected long writeStreamsToBlock() throws IOException {
        long writtenBytes = 0;
        openWritesLock.lock();
        try {
            /* yes, this could be optimized for concurrency by writing after lock is released, etc */
            if (openWriteBytes.get() == 0) {
                return 0;
            }
            List<TempData> streamsWithData = new ArrayList<>(openStreamWrites.size());
            for (StreamOut out : openStreamWrites.values()) {
                synchronized (out) {
                    StreamOut pendingOut = pendingStreamCloses.get(out.meta.streamId);
                    if (pendingOut != null) {
                        pendingOut.outputBuffer.writeBytes(out.outputBuffer);
                        assert out.outputBuffer.readableBytes() == 0;
                        out.outputBuffer.discardSomeReadBytes();
                    } else if (out.output.buffer().readableBytes() > 0) {
                        streamsWithData.add(new TempData(out));
                        out.outputBuffer.retain();
                    }
                }
            }
            for (StreamOut out : pendingStreamCloses.values()) {        // guarded by openStreamWrites
                streamsWithData.add(new TempData(out));
            }
            pendingStreamCloses.clear();
            if (streamsWithData.isEmpty()) {
                return 0;
            }
            for (TempData td : streamsWithData) {
                writtenBytes += td.snapshotLength;
            }
            int streams = streamsWithData.size();
            publishEvent(MuxyStreamEvent.BLOCK_FILE_WRITE, streams);
            int currentFileOffset = (int) openWriteFile.size();
            /* write out IDs in this block */
            ByteBuf metaBuffer = PooledByteBufAllocator.DEFAULT.directBuffer(2 + 4 * streams + 4 + 8 * streams);
            metaBuffer.writeShort(streams);
            int bodyOutputSize = 0;
            for (TempData out : streamsWithData) {
                metaBuffer.writeInt(out.meta.streamId);
                /* (4) chunk body offset (4) chunk length (n) chunk bytes */
                bodyOutputSize += 8 + out.snapshotLength;
            }
            /* write remainder size for rest of block data so that readers can skip if desired ID isn't present */
            metaBuffer.writeInt(bodyOutputSize);
            /* write offsets and lengths for each stream id */
            int bodyOffset = streamsWithData.size() * 8;
            for (TempData out : streamsWithData) {
                metaBuffer.writeInt(bodyOffset); //TODO - reconsider how frequently this shortcut is placed on disk
                metaBuffer.writeInt(out.snapshotLength);
                bodyOffset += out.snapshotLength;
            }
            while (metaBuffer.readableBytes() > 0) {
                metaBuffer.readBytes(openWriteFile, metaBuffer.readableBytes());
            }
            metaBuffer.release();
            /* write bytes for each stream id */
            for (TempData out : streamsWithData) {
                synchronized (out.stream) {     // need less confusing variable names for concurrency
                    int toWrite = out.snapshotLength;
                    while (toWrite > 0) {
                        int numBytesRead = out.data.readBytes(openWriteFile, toWrite);
                        assert numBytesRead > 0;
                        toWrite -= numBytesRead;
                    }
                    openWriteBytes.addAndGet((long) -out.snapshotLength);
                    eventListener.reportWrite((long) -out.snapshotLength);
                    out.meta.endFile = streamDirectoryConfig.currentFile.get();
                    out.meta.endFileBlockOffset = currentFileOffset;
                    if (out.meta.startFile == 0) {
                        out.meta.startFile = out.meta.endFile;
                        out.meta.startFileBlockOffset = out.meta.endFileBlockOffset;
                    }
                    if (!out.data.release()) {      // release the pending writes that did not get an extra retain
                        out.data.discardSomeReadBytes();
                    }
                }
            }
            /* check for rolling current file on size threshold */
            if (openWriteFile.size() > streamDirectoryConfig.maxFileSize) {
                openWriteFile.close();
                openWriteFile = FileChannel.open(getFileByID(bumpCurrentFile()), APPEND, CREATE);
                publishEvent(MuxyStreamEvent.BLOCK_FILE_WRITE_ROLL, streamDirectoryConfig.currentFile);
            }
        } finally {
            openWritesLock.unlock();
        }
        return writtenBytes;
    }

    /* wrapper for writing into chunks */
    protected final class StreamOut {

        final MuxStream meta;
        final AtomicInteger writers = new AtomicInteger(0);
        final ByteBufOutputStream output;
        private final ByteBuf outputBuffer;

        StreamOut(final MuxStream meta) {
            this.meta = meta;
            this.outputBuffer = PooledByteBufAllocator.DEFAULT.ioBuffer(0);
            this.output = new ByteBufOutputStream(outputBuffer);
        }

        public OutputStream getWriter() {
            writers.incrementAndGet();
            return new StreamOutWriter(this);
        }

        void write(int b) throws IOException {
            synchronized (this) {
                if (outputBuffer.capacity() == 0) {
                    outputBuffer.ensureWritable(BUFFER_MIN_SIZE);
                }
                output.write(b);
                openWriteBytes.addAndGet(1);
                meta.bytes += 1;
            }
            eventListener.reportWrite(1);
        }

        void write(final byte[] b, final int off, final int len) throws IOException {
            synchronized (this) {
                if (outputBuffer.capacity() == 0) {
                    outputBuffer.ensureWritable(BUFFER_MIN_SIZE);
                }
                output.write(b, off, len);
                openWriteBytes.addAndGet(len);

                meta.bytes += len;
            }
            eventListener.reportWrite(len);
        }

        void close() throws IOException {
            // no one is writing a new block and no one is getting a new writer
            openWritesLock.lock();
            try {
                publishEvent(MuxyStreamEvent.STREAM_CLOSE, meta);
                if (writers.decrementAndGet() == 0) {       // there are no other valid writers nor will be
                    publishEvent(MuxyStreamEvent.STREAM_CLOSED_ALL, meta);
                    openStreamWrites.remove(meta.streamId);
                    if (openStreamWrites.isEmpty()) {
                        closeTime.set(System.currentTimeMillis());
                        publishEvent(MuxyStreamEvent.CLOSED_ALL_STREAM_WRITERS, meta);
                    }
                    StreamOut existingPend = pendingStreamCloses.get(meta.streamId);
                    if ((existingPend != null) && (existingPend != this)) {     // should never be this?
                        existingPend.outputBuffer.writeBytes(outputBuffer);
                        assert outputBuffer.readableBytes() == 0;
                        outputBuffer.release();
                    } else if (outputBuffer.readableBytes() > 0) {
                        pendingStreamCloses.put(meta.streamId, this);
                        // quick hack to try to prevent number of streams in a block from being > 2^8
                        // TODO: better fix than this hack
                        if (pendingStreamCloses.size() > 1000) {
                            writeStreamsToBlock();
                        }
                    } else {
                        outputBuffer.release();
                    }
                }
            } finally {
                openWritesLock.unlock();
            }
        }
    }

    /* for tracking # of writers per output stream and enforcing close calls */
    protected final class StreamOutWriter extends OutputStream {

        StreamOut out;

        StreamOutWriter(StreamOut out) {
            this.out = out;
        }

        @Override
        public void write(int arg0) throws IOException {
            out.write(arg0);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
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
