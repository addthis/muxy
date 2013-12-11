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
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;

import com.addthis.basis.util.Parameter;

import com.google.common.base.Objects;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * stream multiplexer. allows for a large number of append-only streams
 * to exist inside of a much smaller number of on-disk files. files consist
 * of a series of fast-skip blocks. each block contains a linked list of
 * bytes for 1 or more streams.
 */
public class ReadMuxStreamDirectory {

    private static final Logger log = LoggerFactory.getLogger(ReadMuxStreamDirectory.class);

    protected static final int DEFAULT_MAP_SIZE = Parameter.intValue("muxy.stream.map.default.size", 257);
    // trip-wire to prevent OOMs on too many records in directory
    protected static final int MAX_RECORDS_READ = Parameter.intValue("muxy.stream.max.records", 1000000);

    protected static final DecimalFormat fileFormat = new DecimalFormat("out-00000000");

    protected static String formatFileName(int blockFile) {
        return fileFormat.format(blockFile);
    }

    protected final Path streamDirectory;
    protected final Path dirMetaFile;
    protected final Path dirDataFile;
    protected final Map<Integer, MuxStream> streamDirectoryMap;
    protected final MuxDirectory streamDirectoryConfig;

    protected final AtomicBoolean releaseComplete = new AtomicBoolean(true);
    protected final AtomicLong closeTime = new AtomicLong(0);
    protected FileChannel writeMutexFile;
    protected FileLock writeMutexLock;
    protected MuxyStreamEventListener eventListener;
    protected boolean deleteFreed;

    public ReadMuxStreamDirectory(Path dir) throws Exception {
        this(dir, null);
    }

    public ReadMuxStreamDirectory(Path dir, MuxyStreamEventListener listener) throws Exception {
        this.eventListener = listener;
        this.streamDirectory = dir.toRealPath();
        this.streamDirectoryConfig = new MuxDirectory(this);
        this.dirMetaFile = streamDirectory.resolve("mfs.conf");
        this.dirDataFile = streamDirectory.resolve("mfs.data");
        streamDirectoryConfig.read();
        this.streamDirectoryMap = new HashMap<>(streamDirectoryConfig.streamMapSize);
        readMetaLog();
    }

    protected Path getFileByID(final int fileID) {
        return streamDirectory.resolve(fileFormat.format(fileID));
    }

    /* force new "current" file -- used in defrag operations */
    protected int bumpCurrentFile() throws IOException {
        return streamDirectoryConfig.getNextFile();
    }

    protected int reserveStreamID() throws IOException {
        return streamDirectoryConfig.getNextStreamID();
    }

    public void setMaxBlockSize(int size) throws IOException {
        streamDirectoryConfig.maxBlockSize = size;
        streamDirectoryConfig.write();
    }

    public void setMaxFileSize(int size) throws IOException {
        streamDirectoryConfig.maxFileSize = size;
        streamDirectoryConfig.write();
    }

    protected void publishEvent(MuxyStreamEvent ID, Object target) {
        if (eventListener != null) {
            eventListener.event(ID, target);
        }
    }

    void blockStat() throws Exception {
        int tiny_block = Parameter.intValue("tiny-size", 15000);
        String fileMatch = Parameter.value("file-match", "out-*");
        // Stats to report
        long blocks = 0;
        long chunks = 0;
        Histogram chunkSize = Metrics.newHistogram(ReadMuxStreamDirectory.class, "chunkSize");
        Histogram blockSize = Metrics.newHistogram(ReadMuxStreamDirectory.class, "blockSize");
        Histogram chunksPerBlock = Metrics.newHistogram(ReadMuxStreamDirectory.class, "chunksPerBlock");


        // Get stats
        int currentFile = 1;
        Iterator<Path> dataFiles = Files.newDirectoryStream(streamDirectory, fileMatch).iterator();
        Path lastPath = dataFiles.next();
        long nextBlockPosition = 0;
        RandomAccessFile input = new RandomAccessFile(lastPath.toFile(), "r");
        while (true) {
            if (nextBlockPosition != 0) {
                input.seek(nextBlockPosition);
            }
            if (input.getFilePointer() >= input.length()) {
                input.close();
                if (!dataFiles.hasNext()) {
                    log.info("ran out of mux data files after : " + lastPath.toString());
                    break;
                }
                lastPath = dataFiles.next();
                input = new RandomAccessFile(lastPath.toFile(), "r");
                nextBlockPosition = 0;
            }
            // parse the next block
            long currentBlock = blocks++;
            int countIDs = input.readShort();
            chunks += countIDs;
            chunksPerBlock.update(countIDs);
            ArrayList<Integer> streamList = new ArrayList<>(50);
            for (int i = 0; i < countIDs; i++) {
                int streamID = input.readInt();
                streamList.add(streamID);
            }
            int bodySize = input.readInt();
            long currentPosition = input.getFilePointer();
            long currentBlockStart = nextBlockPosition;
            nextBlockPosition = currentPosition + bodySize;
            long currentBlockSize = nextBlockPosition - currentBlockStart;
            blockSize.update(currentBlockSize);
            if (currentBlockSize < tiny_block) {
                log.info("Tiny block debug log");
                log.info(Objects.toStringHelper("block")
                        .add("block", currentBlock)
                        .add("chunks", countIDs)
                        .add("size", currentBlockSize)
                        .add("os-file", lastPath.getFileName().toString())
                        .add("position", currentPosition)
                        .toString());
                StringBuilder sb = new StringBuilder();
                for (Integer i : streamList) {
                    sb.append(i);
                    sb.append('\n');
                }
                log.info("Stream ids in block : ");
                log.info(sb.toString());
            }
            for (int i = 0; i < countIDs; i++) {
                int chunkBodyOffset = input.readInt(); // throw away
                int chunkLength = input.readInt();
                chunkSize.update(chunkLength);
            }
        }

        // Report stats
        log.info("### Printing stats");
        log.info("Total blocks : " + blocks);
        log.info("Total chunks : " + chunks);
        log.info("Median Chunks per Block : " + chunksPerBlock.getSnapshot().getMedian());
        log.info("05th Percentile Chunks per Block : " + chunksPerBlock.getSnapshot().getValue(0.05));
        log.info("95th Percentile Chunks per Block : " + chunksPerBlock.getSnapshot().get95thPercentile());
        log.info("Median Block Size : " + blockSize.getSnapshot().getMedian());
        log.info("Median Chunk Size : " + chunkSize.getSnapshot().getMedian());
        log.info("05th Percentile Block Size : " + blockSize.getSnapshot().getValue(0.05));
        log.info("05th Percentile Chunk Size : " + chunkSize.getSnapshot().getValue(0.05));
        log.info("95th Percentile Block Size : " + blockSize.getSnapshot().get95thPercentile());
        log.info("95th Percentile Chunk Size : " + chunkSize.getSnapshot().get95thPercentile());
    }

    /* only runs once in constructor */
    protected void readMetaLog() throws IOException {
        int entriesRead = 0;
        if (Files.isRegularFile(dirDataFile)) {
            InputStream in = Files.newInputStream(dirDataFile);
            while (in.available() > 0) {
                try {
                    MuxStream meta = new MuxStream(this, in);
                    streamDirectoryMap.put(meta.streamID, meta);
                    if (entriesRead++ >= MAX_RECORDS_READ) {
                        throw new IOException("max records " + MAX_RECORDS_READ + " exceeded @ " + streamDirectory);
                    }
                } catch (EOFException ex) {
                    log.warn("Hit EOF Exception while reading meta log for : " + streamDirectory, ex);
                    break;
                } catch (Exception ex) {
                    throw new IOException(ex);
                }
            }
            in.close();
        }
        publishEvent(MuxyStreamEvent.LOG_READ, entriesRead);
    }

    public Collection<MuxStream> listStreams() throws IOException {
        return Arrays.asList(streamDirectoryMap.values().toArray(new MuxStream[streamDirectoryMap.size()]));
    }

    public int size() {
        return streamDirectoryMap.size();
    }

    public MuxStream findStream(int streamID) throws IOException {
        MuxStream meta = streamDirectoryMap.get(streamID);
        if (meta == null) {
            throw new IOException("No Such Stream ID " + streamID + " in " + streamDirectory);
        }
        return meta;
    }

    public Collection<Path> getActiveFiles() throws IOException {
        Set<Integer> usedSet = new HashSet<>();
        for (MuxStream metaAdd : streamDirectoryMap.values()) {
            usedSet.add(metaAdd.startFile);
            usedSet.add(metaAdd.endFile);
        }
        Set<Path> open = new HashSet<>();
        for (Integer used : usedSet) {
            open.add(getFileByID(used));
        }
        return open;
    }

    public InputStream readStream(MuxStream meta) throws IOException {
        meta = findStream(meta.streamID);
        if (meta.startFile == 0) {
            throw new IOException("uninitialized stream");
        }
        publishEvent(MuxyStreamEvent.STREAM_READ, meta);
        return new StreamIn(meta);
    }

    /* this is much trickier */
    protected final class StreamIn extends InputStream {

        protected final MuxStream meta;
        protected FileChannel input;
        protected int currentFile;
        protected int currentRemain;
        protected long nextBlockPosition;

        protected StreamIn(MuxStream meta) throws IOException {
            this.meta = meta;
            this.currentFile = meta.startFile;
            input = FileChannel.open(getFileByID(meta.startFile));
            input.position(meta.startFileBlockOffset);
            publishEvent(MuxyStreamEvent.BLOCK_FILE_READ_OPEN, currentFile);
        }

        @Override
        public void close() throws IOException {
            publishEvent(MuxyStreamEvent.BLOCK_FILE_READ_CLOSE, currentFile);
            input.close();
        }

        @Override
        public int available() throws IOException {
            return fill() ? currentRemain : 0;
        }

        /* assumes file pointer is at the beginning of a valid block */
        /* return true if more data is available */
        protected boolean fill() throws IOException {
            while (currentRemain == 0 && currentFile <= meta.endFile) {
                if (currentFile == meta.endFile && input.position() > meta.endFileBlockOffset) {
                    return false;
                }
                if (nextBlockPosition != 0) {
                    input.position(nextBlockPosition);
                }
                if (input.position() >= input.size()) {
                    input.close();
                    publishEvent(MuxyStreamEvent.BLOCK_FILE_READ_CLOSE, currentFile);
                    Path nextFile = getFileByID(++currentFile);
                    if (!Files.exists(nextFile)) {
                        log.warn("terminating stream on missing: {}", nextFile);
                        return false;
                    }
                    input = FileChannel.open(nextFile);
                    nextBlockPosition = 0;
                    publishEvent(MuxyStreamEvent.BLOCK_FILE_READ_OPEN, currentFile);
                }
                // find next block that has this stream id
                ByteBuffer shortBuffer = ByteBuffer.allocate(2);
                while (input.read(shortBuffer) > 0) {
                    ;
                }
                shortBuffer.flip();
                int countIDs = shortBuffer.getShort();

                int bufferSize = Math.min(1024, countIDs);
                ByteBuffer buffer = ByteBuffer.allocateDirect(4 * bufferSize);
                int[] streams = new int[bufferSize];
                int offset = 0;
                int offsetFound = 0;
                while (countIDs > 0) {
                    buffer.clear();
                    if (countIDs < bufferSize) {
                        buffer.limit(countIDs * 4);
                    }
                    while (input.read(buffer) > 0) {
                        ;
                    }
                    buffer.flip();
                    IntBuffer ibuffer = buffer.asIntBuffer();
                    int ibuffRemain = Math.min(ibuffer.remaining(), countIDs);
                    ibuffer.get(streams, 0, ibuffRemain);
                    for (int i = 0; i < ibuffRemain; i++) {
                        if (streams[i] == meta.streamID) {
                            offsetFound = offset + i + 1;
                            //TODO break and seek ahead based on countIDs
                        }
                    }
                    countIDs -= ibuffRemain;
                    offset += ibuffRemain;
                }
                buffer = ByteBuffer.allocate(4);
                while (input.read(buffer) > 0) {
                    ;
                }
                buffer.flip();
                int bodySize = buffer.getInt();
                long currentPosition = input.position();
                nextBlockPosition = currentPosition + bodySize;
                if (offsetFound == 0) {
                    input.position(nextBlockPosition);
                    continue;
                } else {
                    input.position(currentPosition + 8 * (offsetFound - 1));
                }
                ByteBuffer chunkBuffer = ByteBuffer.allocate(8);
                while (input.read(chunkBuffer) > 0) {
                    ;
                }
                chunkBuffer.flip();
                int chunkBodyOffset = chunkBuffer.getInt();
                int chunkBodyLength = chunkBuffer.getInt();
                input.position(currentPosition + chunkBodyOffset);
                currentRemain = chunkBodyLength;
            }
            return currentRemain > 0;
        }

        ByteBuffer singleByte;

        @Override
        public int read() throws IOException {
            if (fill()) {
                if (singleByte == null) {
                    singleByte = ByteBuffer.allocate(1);
                }
                singleByte.clear();
                int read = input.read(singleByte);
                if (read >= 0) {
                    currentRemain--;
                }
                singleByte.flip();
                return singleByte.get() & 0xff;
            }
            return -1;
        }

        @Override
        public int read(final byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        @Override
        public int read(final byte[] b, final int off, final int len) throws IOException {
            if (!fill()) {
                return -1;
            }
            final int read = input.read(ByteBuffer.wrap(b, off, Math.min(len, currentRemain)));
            if (read > 0) {
                currentRemain -= read;
            }
            return read;
        }
    }

}
