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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

import com.addthis.basis.util.Bytes;

import com.google.common.collect.Iterators;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMultiplexedFileStreams {

    private static final Logger log = LoggerFactory.getLogger(TestMultiplexedFileStreams.class);

    private static final String WRITE_TEMPLATE = "<<< xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx >>>";
    private static final String[] CHAR_WRITES = new String[26];
    private static final Set<MuxyStreamEvent> debugEvents = EnumSet.noneOf(MuxyStreamEvent.class);
    private static final Set<MuxyStreamEvent> allEvents = EnumSet.allOf(MuxyStreamEvent.class);

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    @BeforeClass
    public static void addWatchedEvents() {
//        debugEvents.add(MuxyStreamEvent.BLOCK_FILE_FREED);

        int i = 0;
        for (char c = 'a'; c <= 'z'; c++) {
            CHAR_WRITES[i++] = WRITE_TEMPLATE.replace("x", String.valueOf(c));
        }
    }

    @Test
    public void test1() throws Exception {
        File dirFile = tempFolder.newFolder();
        Path dir = dirFile.toPath();
        log.info("test1 TEMP DIR --> {}", dir);
        EventLogger eventLogger = new EventLogger("test1", debugEvents);
        MuxStreamDirectory mfs = new MuxStreamDirectory(dir, eventLogger);
        mfs.setMaxBlockSize(1000);
        mfs.setMaxFileSize(10000);

        MuxStream stream1 = createWriteStream(mfs, 1, 1)[0].stream;
        validateStream(mfs, stream1);

        MuxStream stream2 = createWriteStream(mfs, 1, 1)[0].stream;
        validateStream(mfs, stream2);
        validateStream(mfs, stream1);

        MuxStream stream3 = createWriteStream(mfs, 1, 1)[0].stream;
        MuxStream stream4 = createWriteStream(mfs, 1, 1)[0].stream;
        validateStream(mfs, stream4);
        validateStream(mfs, stream3);
        validateStream(mfs, stream2);
        validateStream(mfs, stream1);

        mfs.waitForWriteClosure();

        log.info("test1.streams.preclose --> {}", mfs.listStreams());

        EventLogger validationEventLogger = new EventLogger("validate-test1", debugEvents);
        ReadMuxStreamDirectory mfs2 = new ReadMuxStreamDirectory(dir, validationEventLogger);
        log.info("test1.streams.postopen --> {}", mfs2.listStreams());
        validateStream(mfs2, stream4);
        validateStream(mfs2, stream3);
        validateStream(mfs2, stream2);
        validateStream(mfs2, stream1);
    }

    @Test
    public void test2() throws Exception {
        File dir = tempFolder.newFolder();
        log.info("test2 TEMP DIR --> {}", dir);
        EventLogger eventLogger = new EventLogger("test2", debugEvents);
        MuxStreamDirectory mfs = new MuxStreamDirectory(dir.toPath(), eventLogger);
        mfs.setMaxBlockSize(50000); // 50K
        mfs.setMaxFileSize(10000000); // 10MB

        int totalStreams = 0;
        int totalChars = 0;

        for (int iter = 1; iter < 10; iter++) {
            for (int conc = 1; conc < 50; conc++) {
                log.debug("test2 ITERATIONS {} CONCURRENCY {}", iter, conc);
                WriteStream[] streams = createWriteStream(mfs, iter, conc);
                for (WriteStream stream : streams) {
                    totalChars += validateStream(mfs, stream.stream);
                    totalStreams++;
                }
            }
        }

        long totalStreamBytes = 0;
        for (MuxStream stream : mfs.listStreams()) {
            totalStreamBytes += stream.getStreamBytes();
            log.debug("test2.stream --> {}", stream);
        }
        log.info("test2 streams {} bytes {}", totalStreams, totalStreamBytes);
        Collection<MuxStream> goodList = mfs.listStreams();

        mfs.waitForWriteClosure();

        log.info("test2 post-re-open validating streams: {}", goodList.size());
        ReadMuxStreamDirectory readStreamDirectory = new ReadMuxStreamDirectory(dir.toPath(), eventLogger);
        for (MuxStream meta : goodList) {
            validateStream(readStreamDirectory, meta);
        }
    }

    @Test
    public void test3() throws Throwable {
        File dir = tempFolder.newFolder();
        log.info("test3 TEMP DIR --> {}", dir);
        final LinkedBlockingQueue<MuxStream> streams = new LinkedBlockingQueue<>();
        EventLogger eventLogger = new EventLogger("test3", debugEvents);
        final MuxStreamDirectory mfs = new MuxStreamDirectory(dir.toPath(), eventLogger);
        mfs.setMaxBlockSize(100 * 1024);
        mfs.setMaxFileSize(10 * 1024 * 1024);

        ExecutorService executor = Executors.newCachedThreadPool();
        Future<?>[] futures = new Future[100];
        for (int i = 0; i < 100; i++) {
            futures[i] = executor.submit(new Callable<Void>() {
                public Void call() throws Exception {
                    MuxStream stream = createWriteStream(mfs, 1000, 1)[0].stream;
                    validateStream(mfs, stream);
                    streams.put(stream);
                    return null;
                }
            });
        }

        executor.shutdown();
        boolean result = executor.awaitTermination(600, TimeUnit.SECONDS);
        Assert.assertTrue("executor did not shut down in 30 seconds", result);
        try {
            for (Future<?> future : futures) {
                future.get();
            }
        } catch (ExecutionException ex) {
            throw ex.getCause();
        }

        int totalStreams = 0;
        long totalStreamBytes = 0;
        for (MuxStream stream : mfs.listStreams()) {
            totalStreams++;
            totalStreamBytes += stream.getStreamBytes();
            log.debug("test3.stream --> {}", stream);
        }
        log.info("test3 streams {} bytes {}", totalStreams, totalStreamBytes);

        mfs.waitForWriteClosure();

        log.info("test3 post-re-open validating streams: {}", streams.size());

        EventLogger validationEventLogger = new EventLogger("validate-test3", debugEvents);
        ReadMuxStreamDirectory mfs2 = new ReadMuxStreamDirectory(dir.toPath(), validationEventLogger);
        for (MuxStream meta : streams) {
            validateStream(mfs2, meta);
        }
    }

    @Test
    public void deleteAll() throws Exception {
        Path dir = tempFolder.newFolder().toPath();
        log.info("deleteAll TEMP DIR --> {}", dir);
        final LinkedBlockingQueue<MuxStream> streams = new LinkedBlockingQueue<>();

        EventLogger eventLogger = new EventLogger("deleteAll", debugEvents);
        final MuxStreamDirectory mfs = new MuxStreamDirectory(dir, eventLogger);
        mfs.setDeleteFreed(true);
        mfs.setMaxBlockSize(100 * 1024);
        mfs.setMaxFileSize(1 * 1024 * 1024);

        for (int loop = 0; loop < 3; loop++) {
            for (int i = 0; i < 10000; i++) {
                streams.add(createWriteStream(mfs, 1, 1)[0].stream);
            }

            Assert.assertEquals("all data files should be active files",
                    dataFileCount(dir), mfs.getActiveFiles().size());

            for (MuxStream meta : streams) {
                mfs.deleteStream(meta.getStreamID());
            }

            mfs.waitForWriteClosure();

            Assert.assertTrue("there should be at most one data file", dataFileCount(dir) <= 1);
            Assert.assertEquals("there should be no active fileIds", 0, mfs.getActiveFiles().size());

            streams.clear();
        }
    }

    private static int dataFileCount(Path dir) throws IOException {
        try(DirectoryStream<Path> dataFiles = Files.newDirectoryStream(dir, "out-*")) {
            return Iterators.size(dataFiles.iterator());
        }
    }

    private static class WriteStream {
        MuxStream stream;
        OutputStream out;
        String template;

        WriteStream(MuxStreamDirectory muxStreamDirectory) throws IOException {
            stream = muxStreamDirectory.createStream();
            out = muxStreamDirectory.appendStream(stream);
            template = "[stream." + stream.getStreamID() + "] ";
        }
    }

    /**
     * create one or more potentially overlapping streams with a test iteration count
     */
    private static WriteStream[] createWriteStream(MuxStreamDirectory muxStreamDirectory, int writes, int streams) throws Exception {
        WriteStream[] writeStreams = new WriteStream[streams];
        for (int i = 0; i < streams; i++) {
            writeStreams[i] = new WriteStream(muxStreamDirectory);
            Bytes.writeInt(writes, writeStreams[i].out);
        }

        for (int i = 0; i < writes; i++) {
            for (WriteStream writeStream : writeStreams) {
                for (String CHAR_WRITE : CHAR_WRITES) {
                    Bytes.writeString(writeStream.template + CHAR_WRITE, writeStream.out);
                }
            }
        }
        for (WriteStream writeStream : writeStreams) {
            writeStream.out.close();
            log.debug("created stream {}", writeStream.stream.getStreamID());
        }
        muxStreamDirectory.writeStreamsToBlock();
        return writeStreams;
    }

    private static int validateStream(ReadMuxStreamDirectory mfs, MuxStream meta) throws Exception {
        try(InputStream in = mfs.readStream(meta)) {
            int writes = Bytes.readInt(in);
            int readString = 0;
            for (int i = 0; i < writes; i++) {
                for (String CHAR_WRITE : CHAR_WRITES) {
                    String read = Bytes.readString(in);
                    readString += read.length();
                    log.debug("read.{} [{}] --> {}", CHAR_WRITE.charAt(0), read.length(), read);
                    Assert.assertTrue("fail contain " + CHAR_WRITE + " in " + read, read.indexOf(CHAR_WRITE) > 0);
                    Assert.assertTrue("fail 'stream." + meta.getStreamID() + "' in " + read,
                            read.indexOf("stream." + meta.getStreamID()) > 0);
                }
            }
            log.debug("validated stream {} of {} chars", meta.getStreamID(), readString);
            return readString;
        }
    }
}
