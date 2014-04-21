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
import java.io.InputStream;
import java.io.OutputStream;

import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import java.nio.file.Path;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Strings;

import com.google.common.io.Files;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMultiplexedFileStreams {

    private static final Logger log = LoggerFactory.getLogger(TestMultiplexedFileStreams.class);

    private final Set<MuxyStreamEvent> debugEvents = EnumSet.noneOf(MuxyStreamEvent.class);
    private final Set<MuxyStreamEvent> allEvents = EnumSet.allOf(MuxyStreamEvent.class);

    @Before
    public void addWatchedEvents() {
//        debugEvents.add(MuxyStreamEvent.LOG_READ);
    }

    public static void deleteDirectory(File dir) {
        deleteDirectory(dir, null);
    }

    public static void deleteDirectoryReport(File dir) {
        LinkedList<File> fail = new LinkedList<>();
        deleteDirectory(dir, fail);
        if (fail.size() > 0) {
            log.info("failed to delete files: {}", fail);
        }
    }

    private static void deleteDirectory(File dir, List<File> fail) {
        File[] files = dir.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            if (file.isDirectory()) {
                deleteDirectory(file);
            }
            if (!file.delete() && fail != null) {
                fail.add(file);
            }
        }
        if (!dir.delete() && fail != null) {
            fail.add(dir);
        }
    }

    @Test
    public void test1() throws Exception {
        File dirFile = Files.createTempDir();
        Path dir = dirFile.toPath();
        log.info("test1 TEMP DIR --> {}", dir);
        EventLogger<MuxyStreamEvent> eventLogger = new EventLogger<>("test1", debugEvents);
        MuxStreamDirectory mfs = new MuxStreamDirectory(dir, eventLogger);
        mfs.setMaxBlockSize(1000);
        mfs.setMaxFileSize(10000);

        MuxStream stream1 = createWriteStream(mfs, 1, 1)[0];
        validateStream(mfs, stream1);

        MuxStream stream2 = createWriteStream(mfs, 1, 1)[0];
        validateStream(mfs, stream2);
        validateStream(mfs, stream1);

        MuxStream stream3 = createWriteStream(mfs, 1, 1)[0];
        MuxStream stream4 = createWriteStream(mfs, 1, 1)[0];
        validateStream(mfs, stream4);
        validateStream(mfs, stream3);
        validateStream(mfs, stream2);
        validateStream(mfs, stream1);

        mfs.waitForWriteClosure();

        log.info("test1.streams.preclose --> {}", mfs.listStreams());

        EventLogger<MuxyStreamEvent> validationEventLogger = new EventLogger<>("validate-test1", debugEvents);
        ReadMuxStreamDirectory mfs2 = new ReadMuxStreamDirectory(dir, validationEventLogger);
        log.info("test1.streams.postopen --> {}", mfs2.listStreams());
        validateStream(mfs2, stream4);
        validateStream(mfs2, stream3);
        validateStream(mfs2, stream2);
        validateStream(mfs2, stream1);

        deleteDirectory(dirFile);
    }

    @Test
    public void test2() throws Exception {
        File dir = Files.createTempDir();
        log.info("test2 TEMP DIR --> {}", dir);
        EventLogger<MuxyStreamEvent> eventLogger = new EventLogger<>("test2", debugEvents);
        MuxStreamDirectory mfs = new MuxStreamDirectory(dir.toPath(), eventLogger);
        mfs.setMaxBlockSize(50000); // 50K
        mfs.setMaxFileSize(10000000); // 10MB

        int totalStreams = 0;
        int totalChars = 0;

        for (int iter = 1; iter < 10; iter++) {
            for (int conc = 1; conc < 50; conc++) {
                log.debug("test2 ITERATIONS {} CONCURRENCY {}", iter, conc);
                MuxStream[] streams = createWriteStream(mfs, iter, conc);
                for (MuxStream stream : streams) {
                    totalChars += validateStream(mfs, stream);
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

        deleteDirectory(dir);
    }

    @Test
    public void test3() throws Throwable {
        File dir = Files.createTempDir();
        log.info("test3 TEMP DIR --> {}", dir);
        final LinkedBlockingQueue<MuxStream> streams = new LinkedBlockingQueue<>();
        EventLogger<MuxyStreamEvent> eventLogger = new EventLogger<>("test3", debugEvents);
        final MuxStreamDirectory mfs = new MuxStreamDirectory(dir.toPath(), eventLogger);
        mfs.setMaxBlockSize(100 * 1024);
        mfs.setMaxFileSize(10 * 1024 * 1024);

        ExecutorService executor = Executors.newCachedThreadPool();
        Future<?>[] futures = new Future[100];
        for (int i = 0; i < 100; i++) {
            futures[i] = executor.submit(new Callable<Void>() {
                public Void call() throws Exception {
                    MuxStream stream = createWriteStream(mfs, 1000, 1)[0];
                    validateStream(mfs, stream);
                    streams.put(stream);
                    return null;
                }
            });
        }

        executor.shutdown();
        boolean result = executor.awaitTermination(30, TimeUnit.SECONDS);
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

        EventLogger<MuxyStreamEvent> validationEventLogger = new EventLogger<>("validate-test3", debugEvents);
        ReadMuxStreamDirectory mfs2 = new ReadMuxStreamDirectory(dir.toPath(), validationEventLogger);
        for (MuxStream meta : streams) {
            validateStream(mfs2, meta);
        }

        deleteDirectory(dir);
    }

    @Test
    public void testDelete() throws Exception {
        File dir = Files.createTempDir();
        log.info("testDelete TEMP DIR --> {}", dir);
        final LinkedBlockingQueue<MuxStream> streams = new LinkedBlockingQueue<>();

        EventLogger<MuxyStreamEvent> eventLogger = new EventLogger<>("testDelete", debugEvents);
        final MuxStreamDirectory mfs = new MuxStreamDirectory(dir.toPath(), eventLogger);
        mfs.setDeleteFreed(true);
        mfs.setMaxBlockSize(100 * 1024);
        mfs.setMaxFileSize(1 * 1024 * 1024);

        for (int loop = 0; loop < 3; loop++) {
            for (int i = 0; i < 10000; i++) {
                streams.add(createWriteStream(mfs, 1, 1)[0]);
            }

            log.info("testDelete.pre files.{} --> {}", loop, Strings.join(dir.listFiles(), "\n-- "));
            log.info("testDelete.pre active.{} --> {}", loop, mfs.getActiveFiles().size());

            for (MuxStream meta : streams) {
                mfs.deleteStream(meta.getStreamID());
            }

            mfs.waitForWriteClosure();
            log.info("testDelete.post files.{} --> {}", loop, Strings.join(dir.listFiles(), "\n-- "));
            log.info("testDelete.post active.{} --> {}", loop, mfs.getActiveFiles().size());
            Assert.assertTrue(mfs.getActiveFiles().size() == 0);

            streams.clear();
        }

        deleteDirectory(dir);
    }

    /**
     * create one or more potentially overlapping streams with a test iteration count
     */
    private static MuxStream[] createWriteStream(MuxStreamDirectory mfs, int iter, int conc) throws Exception {
        MuxStream[] meta = new MuxStream[conc];
        OutputStream[] out = new OutputStream[conc];
        String[] template = new String[conc];
        for (int i = 0; i < meta.length; i++) {
            meta[i] = mfs.createStream();
            out[i] = mfs.appendStream(meta[i]);
            Bytes.writeInt(iter, out[i]);
            template[i] = "[stream." + meta[i].getStreamID() + "] <<< xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx >>>";
        }
        while (iter-- > 0) {
            for (int i = 0; i < meta.length; i++) {
                for (char c = 'a'; c < 'z'; c++) {
                    Bytes.writeString(template[i].replace("x", c + ""), out[i]);
                }
            }
        }
        for (int i = 0; i < meta.length; i++) {
            out[i].close();
            log.debug("created stream {}", meta[i].getStreamID());
        }
        mfs.writeStreamsToBlock();
        return meta;
    }

    private static int validateStream(ReadMuxStreamDirectory mfs, MuxStream meta) throws Exception {
        InputStream in = mfs.readStream(meta);
        int iter = Bytes.readInt(in);
        int readString = 0;
        while (iter-- > 0) {
            for (char c = 'a'; c < 'z'; c++) {
                String read = Bytes.readString(in);
                readString += read.length();
                log.debug("read.{} [{}] --> {}", c, read.length(), read);
                Assert.assertTrue("fail contain " + c + " in " + read, read.indexOf(c) > 0);
                Assert.assertTrue("fail 'stream." + meta.getStreamID() + "' in " + read, read.indexOf("stream." + meta.getStreamID()) > 0);
            }
        }
        in.close();
        log.debug("validated stream {} of {} chars", meta.getStreamID(), readString);
        return readString;
    }
}
