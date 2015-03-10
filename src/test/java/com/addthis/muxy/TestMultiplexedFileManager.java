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

import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import java.nio.file.Path;

import com.addthis.basis.util.LessBytes;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.addthis.muxy.MuxFileDirectoryCache.DEFAULT;

public class TestMultiplexedFileManager {

    private static final Logger log = LoggerFactory.getLogger(TestMultiplexedFileManager.class);

    private static final String WRITE_TEMPLATE = "<<< xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx >>>";
    private static final String[] CHAR_WRITES = new String[26];
    private static final Set<MuxyFileEvent> debugEvents = EnumSet.noneOf(MuxyFileEvent.class);
    private static final Set<MuxyFileEvent> allEvents = EnumSet.allOf(MuxyFileEvent.class);

    private final AtomicInteger nextFileName = new AtomicInteger(0);

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    @BeforeClass
    public static void addWatchedEvents() {
        debugEvents.add(MuxyFileEvent.LOG_READ);
        debugEvents.add(MuxyFileEvent.LOG_COMPACT);
//        debugEvents.add(MuxyFileEvent.CLOSED_ALL_FILE_WRITERS);

        int i = 0;
        for (char c = 'a'; c <= 'z'; c++) {
            CHAR_WRITES[i++] = WRITE_TEMPLATE.replace("x", String.valueOf(c));
        }
    }

    @Test
    public void simpleValidate() throws Exception {
        EventLogger eventLogger = new EventLogger("simpleValidate", allEvents);
        File fileDir = tempFolder.newFolder();
        Path dir = fileDir.toPath();
        log.info("test1 TEMP DIR --> {}", dir);
        MuxFileDirectory mfs = new MuxFileDirectory(dir, eventLogger, DEFAULT);

        WritableMuxFile stream1 = createWriteMetas(mfs, 2, 1)[0].meta;
        validateFile(stream1);

        mfs.waitForWriteClosure();
    }

    @Test
    public void test1() throws Exception {
        EventLogger eventLogger = new EventLogger("test1", allEvents);
        File fileDir = tempFolder.newFolder();
        Path dir = fileDir.toPath();
        log.info("test1 TEMP DIR --> {}", dir);
        MuxFileDirectory mfs = new MuxFileDirectory(dir, eventLogger, DEFAULT);

        WritableMuxFile stream1 = createWriteMetas(mfs, 1, 1)[0].meta;
        validateFile(stream1);

        WritableMuxFile stream2 = createWriteMetas(mfs, 1, 1)[0].meta;
        validateFile(stream2);
        validateFile(stream1);

        WritableMuxFile stream3 = createWriteMetas(mfs, 1, 1)[0].meta;
        WritableMuxFile stream4 = createWriteMetas(mfs, 1, 1)[0].meta;
        validateFile(stream4);
        validateFile(stream3);
        validateFile(stream2);
        validateFile(stream1);

        log.info("test1.files --> {}", mfs.listFiles());
        log.info("test1.streams --> {}", mfs.getStreamManager().listStreams());

        mfs.waitForWriteClosure();
    }

    @Test
    public void test2() throws Exception {
        File fileDir = tempFolder.newFolder();
        Path dir = fileDir.toPath();
        log.info("test2 TEMP DIR --> {}", dir);

        EventLogger eventLogger = new EventLogger("test2", debugEvents);
        MuxFileDirectory mfs = new MuxFileDirectory(dir, eventLogger, DEFAULT);

        MuxFileDirectory.WRITE_THRASHOLD = 1024 * 1024;
        MuxFileDirectory.LAZY_LOG_CLOSE = 250;

        int totalStreams = 0;
        int totalChars = 0;

        for (int iter = 1; iter < 5; iter++) {
            for (int conc = 1; conc < 50; conc++) {
                log.debug("test2 ITERATIONS {} CONCURRENCY {}", iter, conc);
                WriteMeta[] writeMetas = createWriteMetas(mfs, iter, conc);
                for (WriteMeta writeMeta : writeMetas) {
                    totalChars += validateFile(writeMeta.meta);
                    totalStreams++;
                }
            }
        }

        // to force lazy close and log re-init
        Thread.sleep(500);
        long totalStreamBytes = 0;
        log.info("test2 streams {} chars {}", totalStreams, totalChars);
        for (MuxFile stream : mfs.listFiles()) {
            totalStreamBytes += stream.getLength();
            log.debug("test2.file --> {}", stream);
        }
        log.info("test2 files {} bytes {}", totalStreams, totalStreamBytes);
        log.info("test2 streams {}", mfs.getStreamManager().listStreams().size());

        mfs.waitForWriteClosure();
    }

    @Test
    public void test3() throws Throwable {
        File dirFile = tempFolder.newFolder();
        Path dir = dirFile.toPath();
        log.info("test3 TEMP DIR --> {}", dir);
        final LinkedBlockingQueue<WritableMuxFile> streams = new LinkedBlockingQueue<>();

        EventLogger eventLogger = new EventLogger("test3", debugEvents);
        final MuxFileDirectory mfs = new MuxFileDirectory(dir, eventLogger, DEFAULT);
        MuxFileDirectory.WRITE_THRASHOLD = 50 * 1024;
        MuxFileDirectory.LAZY_LOG_CLOSE = 250;

        ExecutorService executor = Executors.newCachedThreadPool();
        Future<?>[] futures = new Future[50];
        for (int i = 0; i < 50; i++) {
            futures[i] = executor.submit(new Callable<Void>() {
                public Void call() throws Exception {
                    WritableMuxFile stream = createWriteMetas(mfs, 1000, 1)[0].meta;
                    validateFile(stream);
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

        // to force lazy close and log re-init
        Thread.sleep(500);
        int totalStreams = 0;
        long totalStreamBytes = 0;
        for (MuxFile stream : mfs.listFiles()) {
            totalStreams++;
            totalStreamBytes += stream.getLength();
            log.info("test3.file --> {}", stream);
        }
        log.info("test3 files {} bytes {}", totalStreams, totalStreamBytes);
        log.info("test3 streams {}", mfs.getStreamManager().listStreams().size());

        mfs.waitForWriteClosure();
    }

    @Test
    public void testExists() throws Exception {
        File dirFile = tempFolder.newFolder();
        Path dir = dirFile.toPath();
        log.info("testExists TEMP DIR --> {}", dir);

        EventLogger eventLogger = new EventLogger("testExists", debugEvents);
        final MuxFileDirectory mfs = new MuxFileDirectory(dir, eventLogger, DEFAULT);
        Assert.assertFalse(mfs.exists("someNewFile"));

        WritableMuxFile stream1 = createWriteMetas(mfs, 1, 1)[0].meta;
        Assert.assertTrue(mfs.exists(stream1.getName()));
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
//          for (MuxStreamDirectory.StreamMeta stream : streams)
//          {
//              mfs.deleteStream(stream.getStreamId());
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

    private static class WriteMeta {
        WritableMuxFile meta;
        OutputStream out;
        String template;

        WriteMeta(MuxFileDirectory mfs, int fileId) throws IOException {
            meta = mfs.openFile("{" + fileId + "}", true);
            out = meta.append();
            template = "[file." + meta.getName() + "] ";
        }
    }

    private WriteMeta[] createWriteMetas(MuxFileDirectory mfs, int writes, int files) throws Exception {
        WriteMeta[] metas = new WriteMeta[files];
        for (int i = 0; i < files; i++) {
            metas[i] = new WriteMeta(mfs, nextFileName.incrementAndGet());
            LessBytes.writeInt(writes, metas[i].out);
        }

        for (int i = 0; i < writes; i++) {
            for (WriteMeta meta : metas) {
                for (String CHAR_WRITE : CHAR_WRITES) {
                    LessBytes.writeString(meta.template + CHAR_WRITE, meta.out);
                }
            }
        }
        for (WriteMeta meta : metas) {
            meta.out.close();
            log.debug("created file {}", meta.meta.getName());
        }
        mfs.getStreamManager().writeStreamsToBlock();
        return metas;
    }

    private static int validateFile(WritableMuxFile meta) throws Exception {
        try(InputStream in = meta.read()) {
            int writes = LessBytes.readInt(in);
            int readString = 0;
            for (int i = 0; i < writes; i++) {
                for (String CHAR_WRITE : CHAR_WRITES) {
                    String read = LessBytes.readString(in);
                    readString += read.length();
                    log.debug("read.{} [{}] --> {}", CHAR_WRITE.charAt(0), read.length(), read);
                    Assert.assertTrue("fail contain " + CHAR_WRITE + " in " + read, read.indexOf(CHAR_WRITE) > 0);
                    Assert.assertTrue("fail 'file." + meta.getName() + "' in " + read,
                            read.indexOf("file." + meta.getName()) > 0);
                }
            }
            log.debug("validated file {} of {} chars", meta.getName(), readString);
            return readString;
        }
    }
}
