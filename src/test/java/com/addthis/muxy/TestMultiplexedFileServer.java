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
import java.io.OutputStream;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Random;

import java.nio.file.Path;

import com.addthis.basis.util.Bytes;
import com.addthis.basis.util.Files;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;


public class TestMultiplexedFileServer {

    private static final Logger log = LoggerFactory.getLogger(TestMultiplexedFileServer.class);

    /**
     * sequentially write then delete a bunch of random length files to a lot of directories.
     * directory assignment for each file is random.  should test directory cache cycling
     * and expose closure race conditions.
     */
    @Test
    public void test1() throws Exception {
        MuxFileDirectory.EXIT_CLOSURE_TIMEOUT = 500;
        MuxFileDirectory.EXIT_CLOSURE_TIMEOUT_FORCE = true;
        MuxFileDirectory.WRITE_CLOSE_GRACE_TIME = 100;
        MuxFileDirectoryCache.cacheTimer = 10;
        MuxFileDirectoryCache.cacheDirMax = 10;
        MuxFileDirectoryCache.cacheStreamMax = 11000;
        MuxFileDirectoryCache.writeCacheDirLinger = 10;

        final int dirCount = 1000;
        final int fileCountPerDir = 5;
        final int switchCount = 100000;
        final int minBytesPerWrite = 100;
        final int maxBytesPerWrite = 200;
        final int maxOpenSetSize = 20;
        final Random rand = new Random(0);
        final LinkedHashSet<OutputStream> openSet = new LinkedHashSet<OutputStream>();

        File tmpDir[] = new File[dirCount];
        for (int i = 0; i < tmpDir.length; i++) {
            tmpDir[i] = Files.createTempDir("tms-", "-" + i);
        }

        log.info("test1 dir=" + dirCount + " file=" + fileCountPerDir + " switch=" + switchCount + " open=" + maxOpenSetSize);
        for (int i = 0; i < switchCount; i++) {
            File dir = tmpDir[(i / dirCount) % dirCount]; //tmpDir[rand.nextInt(dirCount)];
            int file = rand.nextInt(fileCountPerDir);
            byte raw[] = new byte[Math.min(maxBytesPerWrite, rand.nextInt(maxBytesPerWrite) + minBytesPerWrite)];
            int val = i & 0xff;
            Arrays.fill(raw, (byte) val);
            MuxFileDirectory mfm = MuxFileDirectoryCache.getWriteableInstance(dir);
            while (openSet.size() >= maxOpenSetSize) {
                Iterator<OutputStream> iter = openSet.iterator();
                iter.next().close();
                iter.remove();
            }
            try {
                OutputStream out = mfm.openFile(file + "", true).append();
                Bytes.writeLength(raw.length, out);
                out.write(val);
                out.write(raw);
                openSet.add(out);
            } catch (Exception ex) {
                ex.printStackTrace();
                Assert.fail("iter " + i + " fail append " + dir + " / " + file);
            }
            if (i > 0 && i % 1000 == 0) {
                log.info("test1 @ switch " + i +
                                   " open=" + openSet.size() +
                                   " cache.churn=" + MuxFileDirectoryCache.getAndClearCacheEvictions() +
                                   " cache.dir=" + MuxFileDirectoryCache.getCacheDirSize() +
                                   " cache.file=" + MuxFileDirectoryCache.getCacheFileSize() +
                                   " cache.streams=" + MuxFileDirectoryCache.getCacheStreamSize());
            }
        }

        log.info("test1 closing " + openSet.size());
        for (OutputStream out : openSet) {
            out.close();
        }

        log.info("test1 waiting for write closure");
        MuxFileDirectoryCache.waitForWriteClosure();
        if (!MuxFileDirectoryCache.tryClear()) {
            log.info("test1 failed to fully clear dir cache");
        }

        log.info("test1 deleting " + tmpDir.length + " test directories");
        for (File dir : tmpDir) {
            TestMultiplexedFileStreams.deleteDirectory(dir);
        }
    }

    /**
     * test file deletion via active file check.
     */
    @Test
    public void test2() throws Exception {
        final byte raw[] = new byte[4096];
        final File dir = Files.createTempDir();
        final MuxFileDirectory mfm = MuxFileDirectoryCache.getWriteableInstance(dir);

        log.info("test2 writing to " + dir);

        for (int i = 0; i < 100; i++) {
            OutputStream out = mfm.openFile("tmp-" + i, true).append();
            for (int j = 0; j < 1024; j++) {
                out.write(raw);
            }
            out.close();
        }

        MuxFileDirectoryCache.waitForWriteClosure();

        for (int i = 0; i < 100; i++) {
            mfm.openFile("tmp-" + i, false).delete();
        }

        MuxFileDirectoryCache.waitForWriteClosure();
        MuxFileDirectoryCache.tryEvict(mfm);
        TestMultiplexedFileStreams.deleteDirectory(dir);

        Collection<Path> used = mfm.getStreamManager().getActiveFiles();
        assertTrue(used.size() == 0);
    }
}
