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
package com.addthis.muxy.collection;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import com.addthis.muxy.collection.DiskBackedQueue.SyncMode;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestDiskBackedQueue {

    private static final Logger log = LoggerFactory.getLogger(TestDiskBackedQueue.class);

    public static final Serializer<String> serializer = new Serializer<String>() {

        @Override public void toOutputStream(String input, OutputStream output) throws IOException {
            byte[] data = input.getBytes();
            DiskBackedQueueInternals.writeInt(output, data.length);
            output.write(data);
        }

        @Override public String fromInputStream(InputStream input) throws IOException {
            int length = DiskBackedQueueInternals.readInt(input);
            byte[] data = new byte[length];
            input.read(data);
            return new String(data);
        }
    };

    @Test
    public void inMemoryWithoutBackgroundThreads() throws Exception {
        Path path = Files.createTempDirectory("dbq-test");
        DiskBackedQueue.Builder<String> builder = new DiskBackedQueue.Builder<>();
        builder.setPageSize(1024);
        builder.setMemMinCapacity(1024);
        builder.setMemMaxCapacity(1024);
        builder.setSerializer(serializer);
        builder.setPath(path);
        builder.setSyncMode(SyncMode.ALWAYS);
        builder.setNumBackgroundThreads(0);
        builder.setTerminationWait(Duration.ofMinutes(2));
        builder.setShutdownHook(false);
        DiskBackedQueue<String> queue = builder.build();
        queue.put("hello");
        queue.put("world");
        assertEquals("hello", queue.poll());
        assertEquals("world", queue.poll());
        assertNull(queue.poll());
        assertEquals(0, filecount(path));
        queue.close();
    }

    @Test
    public void multiPageWithoutBackgroundThreads() throws Exception {
        Path path = Files.createTempDirectory("dbq-test");
        DiskBackedQueue.Builder<String> builder = new DiskBackedQueue.Builder<>();
        builder.setPageSize(2);
        builder.setMemMinCapacity(2);
        builder.setMemMaxCapacity(2);
        builder.setSerializer(serializer);
        builder.setPath(path);
        builder.setSyncMode(SyncMode.ALWAYS);
        builder.setNumBackgroundThreads(0);
        builder.setShutdownHook(false);
        builder.setTerminationWait(Duration.ofMinutes(2));
        DiskBackedQueue<String> queue = builder.build();
        queue.put("hello");
        queue.put("world");
        queue.put("foo");
        queue.put("barbaz");
        // we cannot serialize a page that is referenced by the readPage
        assertEquals(0, filecount(path));
        assertEquals("hello", queue.poll());
        assertEquals("world", queue.poll());
        assertEquals("foo", queue.poll());
        assertEquals("barbaz", queue.poll());
        assertNull(queue.poll());
        assertEquals(0, filecount(path));
        queue.close();
    }

    @Test
    public void onDiskWithoutBackgroundThreads() throws Exception {
        Path path = Files.createTempDirectory("dbq-test");
        DiskBackedQueue.Builder<String> builder = new DiskBackedQueue.Builder<>();
        builder.setPageSize(2);
        builder.setMemMinCapacity(2);
        builder.setMemMaxCapacity(2);
        builder.setSyncMode(SyncMode.ALWAYS);
        builder.setSerializer(serializer);
        builder.setPath(path);
        builder.setNumBackgroundThreads(0);
        builder.setShutdownHook(false);
        builder.setTerminationWait(Duration.ofMinutes(2));
        DiskBackedQueue<String> queue = builder.build();
        queue.put("hello");
        queue.put("world");
        queue.put("foo");
        queue.put("bar");
        queue.put("baz");
        queue.put("quux");
        assertTrue(filecount(path) > 0);
        assertEquals("hello", queue.poll());
        assertEquals("world", queue.poll());
        assertEquals("foo", queue.poll());
        assertEquals("bar", queue.poll());
        assertEquals("baz", queue.poll());
        assertEquals("quux", queue.poll());
        assertNull(queue.poll());
        queue.close();
    }

    @Test
    public void closeAndReopen() throws Exception {
        Path path = Files.createTempDirectory("dbq-test");
        DiskBackedQueue.Builder<String> builder = new DiskBackedQueue.Builder<>();
        builder.setPageSize(1024);
        builder.setMemMinCapacity(1024);
        builder.setMemMaxCapacity(1024);
        builder.setSerializer(serializer);
        builder.setPath(path);
        builder.setSyncMode(SyncMode.ALWAYS);
        builder.setNumBackgroundThreads(0);
        builder.setShutdownHook(false);
        builder.setTerminationWait(Duration.ofMinutes(2));
        DiskBackedQueue<String> queue = builder.build();
        queue.put("hello");
        queue.put("world");
        queue.close();
        assertTrue(filecount(path) > 0);
        queue = builder.build();
        assertEquals("hello", queue.poll());
        assertEquals("world", queue.poll());
        assertNull(queue.poll());
        queue.close();
    }

    private static int filecount(Path path) {
        return path.toFile().list().length;
    }

    @Test
    public void concurrentReadsWrites() throws Exception {
        for (int i = 1; i <= 4; i++) {
            for (int j = 1; j <= 4; j++) {
                for (int k = 0; k <= 4; k++) {
                    for (int m = 0; m < SyncMode.values().length; m++ ) {
                        concurrentReadsWrites(i, j, k, SyncMode.values()[m], 100_000);
                    }
                }
            }
        }
    }

    private void concurrentReadsWrites(int numReaders, int numWriters,
                                       int numBackgroundThreads, SyncMode mode,
                                       int elements) throws Exception {
        log.info("Testing disk backed queue with {} readers, " +
                 "{} writers, {} background threads, and {} sync mode",
                 numReaders, numWriters, numBackgroundThreads, mode);
        Path path = Files.createTempDirectory("dbq-test");
        DiskBackedQueue.Builder<String> builder = new DiskBackedQueue.Builder<>();
        builder.setPageSize(32);
        builder.setMemMinCapacity(128);
        builder.setMemMaxCapacity(512);
        builder.setSyncMode(mode);
        if (mode == SyncMode.PERIODIC) {
            builder.setSyncInterval(Duration.ofMillis(500));
        }
        builder.setSerializer(serializer);
        builder.setPath(path);
        builder.setNumBackgroundThreads(numBackgroundThreads);
        builder.setShutdownHook(false);
        builder.setTerminationWait(Duration.ofMinutes(2));
        DiskBackedQueue<String> queue = builder.build();
        Thread[] readers = new Thread[numReaders];
        Thread[] writers = new Thread[numWriters];
        AtomicInteger generator = new AtomicInteger();
        AtomicBoolean finishedWriters = new AtomicBoolean();
        WritersPhaser writersPhaser = new WritersPhaser(finishedWriters);
        ConcurrentHashMap<String, String> values = new ConcurrentHashMap<>();
        for (int i = 0; i < numReaders; i++) {
            readers[i] = new Thread(new ReaderTask(values, queue, finishedWriters));
            readers[i].start();
        }
        for (int i = 0; i < numWriters; i++) {
            writers[i] = new Thread(new WriterTask(elements, generator, queue, writersPhaser));
            writers[i].start();
        }
        for (int i = 0; i < numWriters; i++) {
            writers[i].join();
        }
        for (int i = 0; i < numReaders; i++) {
            readers[i].join();
        }
        assertEquals(elements, values.size());
        queue.close();
    }

    private static class WriterTask implements Runnable {

        private final int max;

        private final AtomicInteger generator;

        private final DiskBackedQueue<String> queue;

        private final WritersPhaser phaser;

        WriterTask(int max, AtomicInteger generator,
                   DiskBackedQueue<String> queue, WritersPhaser phaser) {
            this.max = max;
            this.generator = generator;
            this.queue = queue;
            this.phaser = phaser;
            phaser.register();
        }

        @Override public void run() {
            int next;
            try {
                while ((next = generator.getAndIncrement()) < max) {
                    queue.put(Integer.toString(next));
                }
            } catch (IOException ex) {
                fail(ex.toString());
            }
            phaser.arriveAndDeregister();
        }
    }

    private static class WritersPhaser extends Phaser {

        private final AtomicBoolean finishedWriters;

        WritersPhaser(AtomicBoolean finishedWriters) {
            this.finishedWriters = finishedWriters;
        }

        @Override
        public boolean onAdvance(int phase, int registeredParties) {
            finishedWriters.set(true);
            return true;
        }
    }

    private static class ReaderTask implements Runnable {

        private final ConcurrentHashMap<String,String> values;

        private final DiskBackedQueue<String> queue;

        private final AtomicBoolean finishedWriters;

        ReaderTask(ConcurrentHashMap<String,String> values,
                   DiskBackedQueue<String> queue, AtomicBoolean finishedWriters) {
            this.values = values;
            this.queue = queue;
            this.finishedWriters = finishedWriters;
        }

        @Override public void run() {
            try {
                while (true) {
                    if (finishedWriters.get()) {
                        String next = queue.poll();
                        if (next != null) {
                            values.put(next, next);
                        } else {
                            return;
                        }
                    } else {
                        String next = queue.take();
                        assertNotNull(next);
                        values.put(next, next);
                    }
                }
            } catch (Exception ex) {
                fail(ex.toString());
            }
        }
    }

}
