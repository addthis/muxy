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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;

import com.addthis.basis.util.Parameter;

import com.google.common.base.MoreObjects;

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
    protected MuxyEventListener eventListener;
    protected boolean deleteFreed;
    protected int startFile = 1;

    public ReadMuxStreamDirectory(Path dir) throws Exception {
        this(dir, null);
    }

    public ReadMuxStreamDirectory(Path dir, MuxyEventListener listener) throws Exception {
        this.eventListener = listener;
        this.streamDirectory = dir;
        this.streamDirectoryConfig = new MuxDirectory();
        this.dirMetaFile = streamDirectory.resolve("mfs.conf");
        this.dirDataFile = streamDirectory.resolve("mfs.data");
        streamDirectoryConfig.read(dirMetaFile);
        this.streamDirectoryMap = new HashMap<>(streamDirectoryConfig.streamMapSize);
        readMetaLog();
    }

    protected Path getFileByID(int fileID) {
        return getFileByID(streamDirectory, fileID);
    }

    public static Path getFileByID(Path streamDirectory, int fileID) {
        return streamDirectory.resolve(fileFormat.format(fileID));
    }

    protected void publishEvent(MuxyStreamEvent ID, Object target) {
        if (eventListener != null) {
            eventListener.streamEvent(ID, target);
        }
    }

    void blockStat() throws Exception {
        int tiny_block = Parameter.intValue("tiny-size", 15000);
        String fileMatch = Parameter.value("file-match", "out-*");
        // Stats to report
        long blocks = 0;
        long fileBlocks = 0;
        long chunks = 0;
        Histogram chunkSize = Metrics.newHistogram(ReadMuxStreamDirectory.class, "chunkSize");
        Histogram blockSize = Metrics.newHistogram(ReadMuxStreamDirectory.class, "blockSize");
        Histogram chunksPerBlock = Metrics.newHistogram(ReadMuxStreamDirectory.class, "chunksPerBlock");


        // Get stats
        Iterator<Path> dataFiles = Files.newDirectoryStream(streamDirectory, "out-*").iterator();
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
                    log.info("ran out of mux data files after : {}", lastPath.toString());
                    break;
                }
                lastPath = dataFiles.next();
                input = new RandomAccessFile(lastPath.toFile(), "r");
                nextBlockPosition = 0;
            }
            // parse the next block
            int countIDs = input.readShort();
            chunks += countIDs;
            chunksPerBlock.update(countIDs);
            ArrayList<Integer> streamList = new ArrayList<>(50);
            for (int i = 0; i < countIDs; i++) {
                int streamID = input.readInt();
                streamList.add(streamID);
            }
            int bodySize = input.readInt(); // (8 * countIDs) + sum of chunk lengths
            long currentPosition = input.getFilePointer(); // currentBlockStart + 2 + (4 * countIDs) + 4
            long currentBlockStart = nextBlockPosition;
            nextBlockPosition = currentPosition + bodySize;
            long currentBlockSize = nextBlockPosition - currentBlockStart; // 2 + (4 * countIDs) + 4 + bodySize
            blockSize.update(currentBlockSize);
            if (currentBlockSize < tiny_block) {
                log.info("Tiny block debug log");
                log.info(MoreObjects.toStringHelper("block")
                                .add("block", fileBlocks)
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
            fileBlocks += 1;
            blocks += 1;
        }

        // Report stats
        log.info("### Printing stats");
        log.info("Total blocks : {}", blocks);
        log.info("Total chunks : {}", chunks);
        log.info("Median Chunks per Block : {}", chunksPerBlock.getSnapshot().getMedian());
        log.info("05th Percentile Chunks per Block : {}", chunksPerBlock.getSnapshot().getValue(0.05));
        log.info("95th Percentile Chunks per Block : {}", chunksPerBlock.getSnapshot().get95thPercentile());
        log.info("Median Block Size : {}", blockSize.getSnapshot().getMedian());
        log.info("Median Chunk Size : {}", chunkSize.getSnapshot().getMedian());
        log.info("05th Percentile Block Size : {}", blockSize.getSnapshot().getValue(0.05));
        log.info("05th Percentile Chunk Size : {}", chunkSize.getSnapshot().getValue(0.05));
        log.info("95th Percentile Block Size : {}", blockSize.getSnapshot().get95thPercentile());
        log.info("95th Percentile Chunk Size : {}", chunkSize.getSnapshot().get95thPercentile());
    }

    /* only runs once in constructor */
    protected void readMetaLog() throws IOException {
        int entriesRead = 0;
        if (Files.isRegularFile(dirDataFile)) {
            InputStream in = Files.newInputStream(dirDataFile);
            while (in.available() > 0) {
                try {
                    MuxStream meta = new MuxStream(this, in);
                    streamDirectoryMap.put(meta.streamId, meta);
                    if (entriesRead++ >= MAX_RECORDS_READ) {
                        throw new IOException("max records " + MAX_RECORDS_READ + " exceeded @ " + streamDirectory);
                    }
                } catch (EOFException ex) {
                    log.warn("Hit EOF Exception while reading meta log for : {}", streamDirectory, ex);
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
        return new ArrayList<>(streamDirectoryMap.values());
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
        int currentFileId = streamDirectoryConfig.currentFile.get();
        int startFileId   = startFile;
        int[] fileSpansPerStart = new int[currentFileId - startFileId + 1];
        for (MuxStream meta : streamDirectoryMap.values()) {
            fileSpansPerStart[meta.startFile - startFileId] =
                    Math.max(fileSpansPerStart[meta.startFile - startFileId], meta.endFile);
        }
        Set<Path> usedFiles = new HashSet<>(currentFileId);
        int usedFilesLookahead = -1;
        for (int i = 0; i < fileSpansPerStart.length; i++) {
            int length = fileSpansPerStart[i] - i;
            usedFilesLookahead = Math.max(length, usedFilesLookahead);
            usedFilesLookahead -= 1;
            if (usedFilesLookahead >= 0) {
                // file is used
                int fileId = i + startFileId;
                usedFiles.add(getFileByID(fileId));
            }
        }
        return usedFiles;
    }

}
