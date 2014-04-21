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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import java.nio.file.Path;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Strings;

import com.google.common.io.Files;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;


public class TestMultiplexedFileStreams {

    private static final Logger log = LoggerFactory.getLogger(TestMultiplexedFileStreams.class);

    private final boolean verbose1 = false;
    private final boolean verbose2 = false;
    private final boolean verbose3 = false;
    private final boolean verboseDelete = false;
    private final boolean verboseCreate = false;
    private final boolean verboseValidate = false;

    static class StreamEventListener implements MuxyStreamEventListener {

        final boolean verbose;
        final String name;

        StreamEventListener(boolean verbose, String name) {
            this.verbose = verbose;
            this.name = name;
        }

        @Override
        public void event(MuxyStreamEvent event, Object target) {
            if (verbose) {
                log.info("streams." + name + ".event." + event + " --> " + target);
            }
        }
    }

    public static void deleteDirectory(File dir) {
        deleteDirectory(dir, null);
    }

    public static void deleteDirectoryReport(File dir) {
        LinkedList<File> fail = new LinkedList<File>();
        deleteDirectory(dir, fail);
        if (fail.size() > 0) {
            log.info("failed to delete files: " + fail);
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
        log.info("test1 TEMP DIR --> " + dir);
        MuxStreamDirectory mfs = new MuxStreamDirectory(dir, new StreamEventListener(verbose1, "test1"));
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

        log.info("test1.streams.preclose --> " + mfs.listStreams());

        mfs = new MuxStreamDirectory(dir, new StreamEventListener(verbose1, "test1"));
        log.info("test1.streams.postopen --> " + mfs.listStreams());
        validateStream(mfs, stream4);
        validateStream(mfs, stream3);
        validateStream(mfs, stream2);
        validateStream(mfs, stream1);

        // not necessary since only reads were performed since last call
        mfs.waitForWriteClosure();

        deleteDirectory(dirFile);
    }

    @Test
    public void test2() throws Exception {
        File dir = Files.createTempDir();
        log.info("test2 TEMP DIR --> " + dir);
        MuxStreamDirectory mfs = new MuxStreamDirectory(dir.toPath(), new StreamEventListener(verbose2, "test2"));
        mfs.setMaxBlockSize(50000); // 50K
        mfs.setMaxFileSize(10000000); // 10MB

        int totalStreams = 0;
        int totalChars = 0;

        for (int iter = 1; iter < 10; iter++) {
            for (int conc = 1; conc < 50; conc++) {
                if (verbose2) {
                    log.info("test2 ITERATIONS " + iter + " CONCURRENCY " + conc);
                }
                MuxStream[] streams = createWriteStream(mfs, iter, conc);
                for (int i = 0; i < streams.length; i++) {
                    totalChars += validateStream(mfs, streams[i]);
                    totalStreams++;
                }
            }
        }

        long totalStreamBytes = 0;
        for (MuxStream stream : mfs.listStreams()) {
            totalStreamBytes += stream.getStreamBytes();
            if (verbose2) {
                log.info("test2.stream --> " + stream);
            }
        }
        log.info("test2 streams " + totalStreams + " bytes " + totalStreamBytes);
        Collection<MuxStream> goodList = mfs.listStreams();

        mfs.waitForWriteClosure();

        log.info("test2 post-re-open validating streams: " + goodList.size());
        mfs = new MuxStreamDirectory(dir.toPath(), new StreamEventListener(verbose2, "test2"));
        for (MuxStream meta : goodList) {
            validateStream(mfs, meta);
        }

        deleteDirectory(dir);
    }

    @Test
    public void test3() throws Exception {
        File dir = Files.createTempDir();
        log.info("test3 TEMP DIR --> " + dir);
        final LinkedBlockingQueue<MuxStream> streams = new LinkedBlockingQueue<MuxStream>();
        final MuxStreamDirectory mfs = new MuxStreamDirectory(dir.toPath(), new StreamEventListener(verbose3, "test3"));
        mfs.setMaxBlockSize(100 * 1024);
        mfs.setMaxFileSize(10 * 1024 * 1024);

        List<Thread> threads = new LinkedList<Thread>();
        for (int i = 0; i < 100; i++) {
            Thread thread = new Thread() {
                public void run() {
                    try {
                        MuxStream stream = createWriteStream(mfs, 1000, 1)[0];
                        validateStream(mfs, stream);
                        streams.put(stream);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        Assert.fail(ex.getMessage());
                    }
                }
            };
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        int totalStreams = 0;
        long totalStreamBytes = 0;
        for (MuxStream stream : mfs.listStreams()) {
            totalStreams++;
            totalStreamBytes += stream.getStreamBytes();
            if (verbose3) {
                log.info("test3.stream --> " + stream);
            }
        }
        log.info("test3 streams " + totalStreams + " bytes " + totalStreamBytes);

        mfs.waitForWriteClosure();

        log.info("test3 post-re-open validating streams: " + streams.size());
        MuxStreamDirectory mfs2 = new MuxStreamDirectory(dir.toPath(), new StreamEventListener(verbose3, "test4"));
        for (MuxStream meta : streams) {
            validateStream(mfs2, meta);
        }

        deleteDirectory(dir);
    }

    @Test
    public void testDelete() throws Exception {
        File dir = Files.createTempDir();
        log.info("testDelete TEMP DIR --> " + dir);
        final LinkedBlockingQueue<MuxStream> streams = new LinkedBlockingQueue<MuxStream>();
        final MuxStreamDirectory mfs = new MuxStreamDirectory(dir.toPath(), new StreamEventListener(verboseDelete, "testDelete"));
        mfs.setDeleteFreed(true);
        mfs.setMaxBlockSize(100 * 1024);
        mfs.setMaxFileSize(1 * 1024 * 1024);

        for (int loop = 0; loop < 3; loop++) {
            for (int i = 0; i < 10000; i++) {
                streams.add(createWriteStream(mfs, 1, 1)[0]);
            }

            log.info("testDelete.pre files." + loop + " --> " + Strings.join(dir.listFiles(), "\n-- "));
            log.info("testDelete.pre active." + loop + " --> " + mfs.getActiveFiles().size());

            for (MuxStream meta : streams) {
                mfs.deleteStream(meta.getStreamID());
            }

            mfs.waitForWriteClosure();
            log.info("testDelete.post files." + loop + " --> " + Strings.join(dir.listFiles(), "\n-- "));
            log.info("testDelete.post active." + loop + " --> " + mfs.getActiveFiles().size());
            assertTrue(mfs.getActiveFiles().size() == 0);

            streams.clear();
        }

        deleteDirectory(dir);
    }

    /**
     * create one or more potentially overlapping streams with a test iteration count
     */
    private MuxStream[] createWriteStream(MuxStreamDirectory mfs, int iter, int conc) throws Exception {
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
            if (verboseCreate) {
                log.info("created stream " + meta[i].getStreamID());
            }
        }
        mfs.writeStreamsToBlock();
        return meta;
    }

    private int validateStream(MuxStreamDirectory mfs, MuxStream meta) throws Exception {
        InputStream in = mfs.readStream(meta);
        int iter = Bytes.readInt(in);
        int readString = 0;
        while (iter-- > 0) {
            for (char c = 'a'; c < 'z'; c++) {
                String read = Bytes.readString(in);
                readString += read.length();
                if (verboseValidate) {
                    log.info("read." + c + " [" + read.length() + "] --> " + read);
                }
                Assert.assertTrue("fail contain " + c + " in " + read, read.indexOf(c) > 0);
                Assert.assertTrue("fail 'stream." + meta.getStreamID() + "' in " + read, read.indexOf("stream." + meta.getStreamID()) > 0);
            }
        }
        in.close();
        if (verboseValidate) {
            log.info("validated stream " + meta.getStreamID() + " of " + readString + " chars");
        }
        return readString;
    }
}
