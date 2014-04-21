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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import java.nio.file.Path;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;


public class TestMultiplexedFileManager {

    private static final Logger log = LoggerFactory.getLogger(TestMultiplexedFileManager.class);

    private static final boolean verbose1 = true;
    private static final boolean verbose2 = false;
    private static final boolean verbose3 = false;
    private static final boolean verboseCreate = false;
    private static final boolean verboseValidate = false;
    private static final AtomicInteger nextFileName = new AtomicInteger(0);
    private static final Set<MuxyFileEvent> watchedEvents = new HashSet<>();

    static {
        watchedEvents.add(MuxyFileEvent.LOG_READ);
        watchedEvents.add(MuxyFileEvent.LOG_COMPACT);
        watchedEvents.add(MuxyFileEvent.CLOSED_ALL_FILE_WRITERS);
    }

    static class FileEventListener implements MuxyFileEventListener {

        final boolean verbose;
        final String name;

        FileEventListener(boolean verbose, String name) {
            this.verbose = verbose;
            this.name = name;
        }

        @Override
        public void event(MuxyFileEvent event, Object target) {
            if (verbose || watchedEvents.contains(event)) {
                if (event == MuxyFileEvent.STREAM_EVENT) {
                    Object[] o = (Object[]) target;
                    log.info(name + ".event.stream --> " + o[0] + " --> " + o[1]);
                } else {
                    log.info("file." + name + ".event." + event + " --> " + target);
                }
            }
        }
    }

    @Test
    public void test1() throws Exception {
        File fileDir = Files.createTempDir();
        Path dir = fileDir.toPath();
        log.info("test1 TEMP DIR --> " + dir);
        MuxFileDirectory mfs = new MuxFileDirectory(dir, new FileEventListener(verbose1, "test1"));

        MuxFile stream1 = createFileStream(mfs, 1, 1)[0];
        validateFile(stream1);

        MuxFile stream2 = createFileStream(mfs, 1, 1)[0];
        validateFile(stream2);
        validateFile(stream1);

        MuxFile stream3 = createFileStream(mfs, 1, 1)[0];
        MuxFile stream4 = createFileStream(mfs, 1, 1)[0];
        validateFile(stream4);
        validateFile(stream3);
        validateFile(stream2);
        validateFile(stream1);

        log.info("test1.files --> " + mfs.listFiles());
        log.info("test1.streams --> " + mfs.getStreamManager().listStreams());

        mfs.waitForWriteClosure();

        Files.deleteDir(dir.toFile());
    }

    @Test
    public void test2() throws Exception {
        File fileDir = Files.createTempDir();
        Path dir = fileDir.toPath();
        log.info("test2 TEMP DIR --> " + dir);
        MuxFileDirectory mfs = new MuxFileDirectory(dir, new FileEventListener(verbose2, "test2"));
        MuxFileDirectory.WRITE_THRASHOLD = 1024 * 1024;
        MuxFileDirectory.LAZY_LOG_CLOSE = 250;

        int totalStreams = 0;
        int totalChars = 0;

        for (int iter = 1; iter < 5; iter++) {
            for (int conc = 1; conc < 50; conc++) {
                if (verbose2) {
                    log.info("test2 ITERATIONS " + iter + " CONCURRENCY " + conc);
                }
                MuxFile[] streams = createFileStream(mfs, iter, conc);
                for (MuxFile stream : streams) {
                    totalChars += validateFile(stream);
                    totalStreams++;
                }
            }
        }

        // to force lazy close and log re-init
        Thread.sleep(500);
        long totalStreamBytes = 0;
        log.info("test2 streams " + totalStreams + " chars " + totalChars);
        for (ReadMuxFile stream : mfs.listFiles()) {
            totalStreamBytes += stream.getLength();
            if (verbose2) {
                log.info("test2.file --> " + stream);
            }
        }
        log.info("test2 files " + totalStreams + " bytes " + totalStreamBytes);
        log.info("test2 streams " + mfs.getStreamManager().listStreams().size());

        mfs.waitForWriteClosure();
        TestMultiplexedFileStreams.deleteDirectory(dir.toFile());
    }

    @Test
    public void test3() throws Exception {
        File dirFile = Files.createTempDir();
        Path dir = dirFile.toPath();
        log.info("test3 TEMP DIR --> " + dir);
        final LinkedBlockingQueue<MuxFile> streams = new LinkedBlockingQueue<>();
        final MuxFileDirectory mfs = new MuxFileDirectory(dir, new FileEventListener(verbose3, "test3"));
        MuxFileDirectory.WRITE_THRASHOLD = 50 * 1024;
        MuxFileDirectory.LAZY_LOG_CLOSE = 250;

        List<Thread> threads = new LinkedList<>();
        for (int i = 0; i < 50; i++) {
            Thread thread = new Thread() {
                public void run() {
                    try {
                        MuxFile stream = createFileStream(mfs, 1000, 1)[0];
                        validateFile(stream);
                        streams.put(stream);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            };
            threads.add(thread);
            thread.start();
        }
        log.info("test 3 started " + threads.size() + " threads");

        for (Thread thread : threads) {
            thread.join();
        }

        // to force lazy close and log re-init
        Thread.sleep(500);
        int totalStreams = 0;
        long totalStreamBytes = 0;
        for (ReadMuxFile stream : mfs.listFiles()) {
            totalStreams++;
            totalStreamBytes += stream.getLength();
            log.info("test3.file --> " + stream);
        }
        log.info("test3 files " + totalStreams + " bytes " + totalStreamBytes);
        log.info("test3 streams " + mfs.getStreamManager().listStreams().size());

        mfs.waitForWriteClosure();
        TestMultiplexedFileStreams.deleteDirectory(dir.toFile());
    }

    @Test
    public void testExists() throws Exception {
        File dirFile = Files.createTempDir();
        Path dir = dirFile.toPath();
        log.info("testExists TEMP DIR --> " + dir);
        final MuxFileDirectory mfs = new MuxFileDirectory(dir, new FileEventListener(verbose3, "testExists"));
        assertFalse(mfs.exists("someNewFile"));

        MuxFile stream1 = createFileStream(mfs, 1, 1)[0];
        assertTrue(mfs.exists(stream1.getName()));
    }

//  @Test  TODO copied from stream test ... rework for files
//  public void testDelete() throws Exception
//  {
//      File dir = com.google.common.io.Files.createTempDir();
//      log.info("testDelete TEMP DIR --> "+dir);
//      final LinkedBlockingQueue<MuxStreamDirectory.StreamMeta> streams = new LinkedBlockingQueue<MuxStreamDirectory.StreamMeta>();
//      final MuxStreamDirectory mfs = new MuxStreamDirectory(dir, new StreamEventListener(verboseDelete, "testDelete"));
//      mfs.setDeleteFreed(true);
//      mfs.setMaxBlockSize(100 * 1024);
//      mfs.setMaxFileSize(1 * 1024 * 1024);
//
//      for (int loop=0; loop<3; loop++)
//      {
//          for (int i=0; i<10000; i++)
//          {
//              streams.add(createWriteStream(mfs,1,1)[0]);
//          }
//
//          log.info("testDelete.pre files."+loop+" --> " + Strings.join(dir.listFiles(), "\n-- "));
//          log.info("testDelete.pre active."+loop+" --> " + mfs.getActiveFiles().size());
//
//          for (MuxStreamDirectory.StreamMeta meta : streams)
//          {
//              mfs.deleteStream(meta.getStreamID());
//          }
//
//          mfs.waitForWriteClosure();
//          log.info("testDelete.post files." + loop + " --> " + Strings.join(dir.listFiles(), "\n-- "));
//          log.info("testDelete.post active."+loop+" --> " + mfs.getActiveFiles().size());
//          Assert.assertTrue(mfs.getActiveFiles().size() == 0);
//
//          streams.clear();
//      }
//
//      deleteDirectory(dir);
//  }

    private MuxFile[] createFileStream(MuxFileDirectory mfs, int iter, int conc) throws Exception {
        MuxFile[] meta = new MuxFile[conc];
        OutputStream[] out = new OutputStream[conc];
        String[] template = new String[conc];
        for (int i = 0; i < meta.length; i++) {
            meta[i] = mfs.openFile("{" + nextFileName.incrementAndGet() + "}", true);
            out[i] = meta[i].append();
            Bytes.writeInt(iter, out[i]);
            template[i] = "[file." + meta[i].getName() + "] <<< xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx >>>";
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
                log.info("created file " + meta[i].getName());
            }
        }
        mfs.writeStreamMux.writeStreamsToBlock();
        return meta;
    }

    private int validateFile(MuxFile meta) throws Exception {
        InputStream in = meta.read(0);
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
                Assert.assertTrue("fail 'file." + meta.getName() + "' in " + read, read.indexOf("file." + meta.getName()) > 0);
            }
        }
        in.close();
        if (verboseValidate) {
            log.info("validated file " + meta.getName() + " of " + readString + " chars");
        }
        return readString;
    }
}
