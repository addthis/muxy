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

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import java.nio.file.Path;
import java.time.Duration;

import com.addthis.muxy.collection.DiskBackedQueue.SyncMode;
import com.addthis.muxy.MuxFile;
import com.addthis.muxy.MuxFileDirectory;
import com.addthis.muxy.MuxyEventListener;
import com.addthis.muxy.MuxyFileEvent;
import com.addthis.muxy.MuxyStreamEvent;
import com.addthis.muxy.WritableMuxFile;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DiskBackedQueueInternals<E> implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(DiskBackedQueue.class);

    private final int pageSize;

    private final int minReadPages;

    private final int minWritePages;

    private final int maxPages;

    private final SyncMode syncMode;

    private final Duration terminationWait;

    private final ReentrantLock lock;

    /**
     * The mux file where external pages are stored.
     * The current implementation uses {@code synchronized {external}}
     * code blocks to coordinate across consumers and producers
     * of the mux file.
     */
    @GuardedBy("external")
    private final MuxFileDirectory external;

    /**
     * Defined as {@code lock.newCondition()}.
     * Is signalled whenever an element is inserted
     * into the queue or when a page is read
     * from disk.
     */
    @GuardedBy("lock")
    private final Condition notEmpty;

    /**
     * Pages waiting to be evicted to disk. Any page
     * in the disk queue must not referenced by the
     * {@code writePage} or the {@code readPage}.
     */
    private final ConcurrentSkipListMap<Long, Page> diskQueue;

    /**
     * Estimate the current size of the diskQueue.
     */
    private final AtomicInteger diskQueueSize;

    /**
     * Pages pulled from the disk and waiting to
     * placed into the {@code readPage}.
     */
    @GuardedBy("lock")
    private final NavigableMap<Long, Page> readQueue;

    @GuardedBy("lock")
    private Page writePage;

    @GuardedBy("lock")
    private Page readPage;

    private final AtomicReference<IOException> error;

    private final Serializer<E> serializer;

    private final ScheduledExecutorService backgroundTasks;

    private final CompletableFuture<Void> closeFuture;

    private final AtomicBoolean closed;

    /**
     * Use {@link DiskBackedQueue.Builder} to construct a disk-backed queue.
     * Throws an exception the mux directory cannot be created or opened.
     * Throws an exception if {@code shutdownHook} is true and the JVM is
     * currently shutting down.
     */
    DiskBackedQueueInternals(int pageSize, int minPages, int maxPages, int numBackgroundThreads,
                             SyncMode syncMode, Duration syncInterval,
                             Path path, Serializer<E> serializer,
                             Duration terminationWait, boolean shutdownHook) throws Exception {
        this.pageSize = pageSize;
        this.maxPages = maxPages;
        this.external = new MuxFileDirectory(path, new NextReadPageListener());
        this.serializer = serializer;
        this.syncMode = syncMode;
        this.lock = new ReentrantLock();
        this.notEmpty = lock.newCondition();
        this.closeFuture = new CompletableFuture<>();
        this.closed = new AtomicBoolean();
        this.terminationWait = terminationWait;
        if (maxPages <= 1) {
            this.diskQueue = null;
            this.diskQueueSize = null;
            this.backgroundTasks = null;
            this.minReadPages = minPages;
            this.minWritePages = 0;
        } else {
            this.diskQueue = new ConcurrentSkipListMap<>();
            this.diskQueueSize = new AtomicInteger();
            this.backgroundTasks = new ScheduledThreadPoolExecutor(
                    numBackgroundThreads + ((syncMode == SyncMode.PERIODIC) ? 1 : 0),
                    new ThreadFactoryBuilder().setNameFormat("disk-backed-queue-writer-%d").build());
            this.minReadPages = Math.max(minPages / 2, 1); // always fetch at least one page
            this.minWritePages = minPages / 2;
            for (int i = 0; i < numBackgroundThreads; i++) {
                backgroundTasks.scheduleWithFixedDelay(new DiskWriteTask(),
                                                       (1000 * i) / numBackgroundThreads,
                                                       1000, TimeUnit.MILLISECONDS);
            }
            if (syncMode == SyncMode.PERIODIC) {
                backgroundTasks.scheduleWithFixedDelay(new DiskSyncTask(),
                                                       syncInterval.toMillis(),
                                                       syncInterval.toMillis(),
                                                       TimeUnit.MILLISECONDS);
            }
        }
        this.readQueue = new TreeMap<>();
        this.error = new AtomicReference<>(null);
        Collection<MuxFile> files = external.listFiles();
        Optional<MuxFile> minFile = files.stream().min((f1, f2) -> (f1.getName().compareTo(f2.getName())));
        Optional<MuxFile> maxFile = files.stream().max((f1, f2) -> (f2.getName().compareTo(f1.getName())));
        if (minFile.isPresent() && maxFile.isPresent()) {
            long readPageId, writePageId;
            readPageId  = Long.parseLong(minFile.get().getName());
            writePageId = Long.parseLong(maxFile.get().getName()) + 1;
            NavigableMap<Long, Page> readPages = readPagesFromExternal(readPageId, minReadPages);
            readPage = readPages.remove(readPageId);
            readQueue.putAll(readPages);
            writePage = new Page(writePageId);
        } else {
            writePage = new Page(0);
            readPage = writePage;
        }
        if (shutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread(this::close, "disk-backed-queue-shutdown"));
        }
    }

    @VisibleForTesting
    static int readInt(InputStream stream) throws IOException {
        byte[] data = new byte[4];
        ByteStreams.readFully(stream, data);
        return Ints.fromByteArray(data);
    }

    @VisibleForTesting
    static void writeInt(OutputStream stream, int val) throws IOException {
        byte[] data = Ints.toByteArray(val);
        stream.write(data);
    }

    /**
     * Fixed length circular buffer of elements.
     */
    private class Page {

        final long id;

        final Object[] elements;

        int readerIndex;

        int writerIndex;

        int count;

        Page(long id, InputStream stream) throws IOException {
            try {
                this.id = id;
                this.elements = new Object[pageSize];
                this.count = readInt(stream);
                for (int i = 0; i < count; i++) {
                    elements[i] = serializer.fromInputStream(stream);
                }
                this.readerIndex = 0;
                this.writerIndex = count;
            } finally {
                stream.close();
            }
        }

        Page(long id) {
            this.id = id;
            this.elements = new Object[pageSize];
            this.count = 0;
            this.readerIndex = 0;
            this.writerIndex = 0;
        }

        boolean empty() {
            return (count == 0);
        }

        boolean full() {
            return (count == pageSize);
        }

        void add(E e) {
            assert(!full());
            elements[writerIndex] = e;
            writerIndex = (writerIndex + 1) % pageSize;
            count++;
        }

        void clear() {
            count = 0;
            readerIndex = 0;
            writerIndex = 0;
        }

        E remove() {
            assert(!empty());
            E result = (E) elements[readerIndex];
            readerIndex = (readerIndex + 1) % pageSize;
            count--;
            return result;
        }

        void writeToFile() throws IOException {
            assert(!empty());
            synchronized (external) {
                WritableMuxFile file = external.openFile(Long.toString(id), true);
                assert(file.getLength() == 0);
                OutputStream outputStream = file.append();
                try {
                    writeInt(outputStream, count);
                    for (int i = 0; i < count; i++) {
                        E next = (E) elements[(readerIndex + i) % pageSize];
                        serializer.toOutputStream(next, outputStream);
                    }
                } finally {
                    outputStream.close();
                }
                if (syncMode == SyncMode.ALWAYS) {
                    file.sync();
                }
            }
        }
    }

    private class DiskWriteTask implements Runnable {

        @Override public void run() {
            while ((getError() == null) && (diskQueueSize.get() > minWritePages)) {
                try {
                    Map.Entry<Long,Page> minEntry = diskQueue.pollFirstEntry();
                    if (minEntry != null) {
                        diskQueueSize.decrementAndGet();
                        minEntry.getValue().writeToFile();
                    } else {
                        break;
                    }
                    if (Thread.interrupted()) {
                        break;
                    }
                } catch (IOException ex) {
                    setError(ex);
                }
            }
        }
    }

    private class DiskSyncTask implements Runnable {
        @Override public void run() {
            if (getError() != null) {
                try {
                    synchronized(external) {
                        external.sync();
                    }
                } catch (IOException ex) {
                    setError(ex);
                }
            }
        }
    }

    private boolean drainQueue(NavigableMap<Long,Page> queue, long endTime, boolean diskQueue) throws IOException {
        if ((endTime > 0) && (System.currentTimeMillis() >= endTime)) {
            return false;
        }
        Map.Entry<Long,Page> minEntry;
        while ((minEntry = queue.pollFirstEntry()) != null) {
            if (diskQueue) {
                diskQueueSize.decrementAndGet();
            }
            minEntry.getValue().writeToFile();
            if ((endTime > 0) && (System.currentTimeMillis() > endTime)) {
                return false;
            }
        }
        return true;
    }

    /**
     * It is possible that a consumer will be looking for the
     * next page to read and concurrently a writer will have removed
     * the page from the disk queue before the consumer checked the queue
     * and wrote the page to disk after the consumer checked the disk.
     * This listener will wake up any consumers under those circumstances.
     */
    private class NextReadPageListener implements MuxyEventListener {

        @Override public void fileEvent(MuxyFileEvent event, Object target) {
            if (event != MuxyFileEvent.FILE_CREATE) {
                return;
            }
            /**
             * This listener is holding the synchronized (external) lock.
             * To prevent deadlock we acquire the {@code lock} object
             * in another thread.
             */
            CompletableFuture.runAsync(new NextReadPageRunnable(((MuxFile) target).getName()));
        }

        @Override public void streamEvent(MuxyStreamEvent event, Object target) {}

        @Override public void reportWrite(long bytes) {}

        @Override public void reportStreams(long streams) {}
    }

    /**
     * Signal to any outstanding consumers that additional nodes
     * are available for consuming. Performed in a separate thread
     * to prevent deadlock.
     */
    private class NextReadPageRunnable implements Runnable {

        private final String muxFileName;

        NextReadPageRunnable(String muxFileName) {
            this.muxFileName = muxFileName;
        }

        public void run() {
            lock.lock();
            try {
                if (readPage.empty() &&
                    Long.toString(readPage.id + 1).equals(muxFileName)) {
                    notEmpty.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private NavigableMap<Long,Page> readPagesFromExternal(long id, int count) throws IOException {
        NavigableMap<Long,Page> results = new TreeMap<>();
        synchronized (external) {
            for(long i = id; i < (id + count); i++) {
                if (!external.exists(Long.toString(i))) {
                    return results;
                }
                WritableMuxFile file = external.openFile(Long.toString(i), false);
                Page page = new Page(i, file.read());
                results.put(i, page);
                file.delete();
            }
        }
        return results;
    }

    /**
     * To prevent a thundering herd of page loads only a single
     * thread is permitted to load at any time.
     */
    private final Semaphore loadPageSemaphore = new Semaphore(1);

    /**
     * Load a page from the file. Returns true if
     * one or more elements are available for reading.
     */
    private boolean loadPageFromFile(long nextId) throws IOException {
        assert(lock.isHeldByCurrentThread());
        /**
         * If the page load semaphore was contested and no elements
         * were available when we reacquired the lock then retry
         * the page load semaphore until either we are the winner
         * of the page load semaphore or an element is available.
         */
        NavigableMap<Long,Page> loadedPages;
        lock.unlock();
        loadPageSemaphore.acquireUninterruptibly();
        try {
            loadedPages = readPagesFromExternal(nextId, minReadPages);
        } finally {
            loadPageSemaphore.release();
            lock.lock();
        }
        if (loadedPages != null) {
            readQueue.putAll(loadedPages);
        }
        if (!readPage.empty()) {
            return true;
        }
        // lock was released and reacquired so we must recalculate nextId
        nextId = readPage.id + 1;
        if (fetchFromQueues(nextId)) {
            return true;
        }
        if ((nextId == writePage.id) && (readPage != writePage)) {
            readPage = writePage;
            return !readPage.empty();
        }
        return false;
    }

    private boolean fetchFromQueues(long id) {
        boolean success = fetchFromQueue(readQueue, id, false);
        if (success) {
            return true;
        } else if (diskQueue == null) {
            return false;
        } else {
            return fetchFromQueue(diskQueue, id, true);
        }
    }

    /**
     * Retrieve a target page from either the readQueue or the diskQueue.
     * If we remove from the disk queue then we must remember to update
     * its size estimate.
     *
     * @param queue      queue to remove page
     * @param id         id of the target page
     * @param diskQueue  if true then update disk queue size
     * @return true if the target page was found
     **/
    private boolean fetchFromQueue(NavigableMap<Long,Page> queue, long id, boolean diskQueue) {
        assert(lock.isHeldByCurrentThread());
        assert(readPage.empty());
        assert(id == (readPage.id + 1));
        Page nextPage = queue.remove(id);
        if (nextPage != null) {
            if (diskQueue) {
                diskQueueSize.decrementAndGet();
            }
            readPage = nextPage;
            notEmpty.signalAll();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Retrieves an element from the end of the queue.
     * For an unbounded waiting call with {@code unit} as null.
     * To return immediately with no waiting call with {@code unit} as
     * non-null and {@code timeout} as 0.
     *
     * @param timeout amount of time to wait. Ignored if {@code unit} is null.
     * @param unit    If non-null then maximum time to wait for an element.
     */
    E get(long timeout, TimeUnit unit) throws InterruptedException, IOException {
        if (closed.get()) {
            throw new IllegalStateException("attempted get() after close()");
        }
        propagateError();
        /**
         * Follow example in
         * https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Condition.html#awaitNanos-long-
         * to handle spurious wakeups.
         */
        long nanos = (unit == null) ? 0 : unit.toNanos(timeout);
        lock.lock();
        try {
            while (true) {
                long nextId = readPage.id + 1;
                if (!readPage.empty()) {
                    return readPage.remove();
                } else if (fetchFromQueues(nextId)) {
                    return readPage.remove();
                } else if ((nextId == writePage.id) && (readPage != writePage)) {
                    readPage = writePage;
                } else if ((nextId < writePage.id) && loadPageFromFile(nextId)) {
                    return readPage.remove();
                } else if (unit == null) {
                    notEmpty.await();
                } else if (nanos <= 0L) {
                    return null;
                } else {
                    nanos = notEmpty.awaitNanos(nanos);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Inserts the specified element into this queue, waiting if necessary
     * for space to become available.
     *
     * @param e the element to add
     * @throws NullPointerException if the specified element is null
     * @throws IllegalArgumentException if some property of the specified
     *         element prevents it from being added to this queue
     * @throws IOException if error reading the backing store
     */
    void put(E e) throws IOException {
        if (closed.get()) {
            throw new IllegalStateException("attempted put() after close()");
        }
        Preconditions.checkNotNull(e);
        propagateError();
        lock.lock();
        try {
            if (!writePage.full()) {
                writePage.add(e);
            } else {
                Page oldPage = writePage;
                writePage = new Page(oldPage.id + 1);
                writePage.add(e);
                if (readPage != oldPage) {
                    if ((diskQueue == null) || (diskQueueSize.get() > maxPages)) {
                        foregroundWrite(oldPage);
                    } else {
                        Page previous = diskQueue.put(oldPage.id, oldPage);
                        assert(previous == null);
                        diskQueueSize.incrementAndGet();
                    }
                }
            }
            // signal if the new element is the next in FIFO order for the consumers
            if ((((readPage.id + 1) == writePage.id) && readPage.empty()) ||
                ((readPage == writePage) && (writePage.count == 1))) {
                notEmpty.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    private void foregroundWrite(Page page) throws IOException {
        assert(lock.isHeldByCurrentThread());
        lock.unlock();
        try {
            page.writeToFile();
        } finally {
            lock.lock();
        }
    }

    private IOException getError() {
        return error.get();
    }

    private void setError(IOException ex) {
        error.compareAndSet(null, ex);
    }

    private void propagateError() throws IOException {
        IOException ex = error.get();
        if (ex != null) {
            throw ex;
        }
    }

    @Override public void close() {
        if (!closed.getAndSet(true)) {
            long startTime = System.currentTimeMillis();
            long endTime = startTime + terminationWait.toMillis();
            try {
                if (backgroundTasks != null) {
                    backgroundTasks.shutdown();
                    try {
                        log.info("Waiting on background threads to write approximately {} pages", diskQueueSize.get());
                        backgroundTasks.awaitTermination(terminationWait.toMillis(), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException ex) {
                        closeFuture.completeExceptionally(ex);
                        Throwables.propagate(ex);
                    } finally {
                        backgroundTasks.shutdownNow();
                    }
                }
                int unwritten = calculateDirtyPageCount();
                log.info("Foreground thread must write approximately {} pages", unwritten);
                boolean hasTime = (System.currentTimeMillis() < endTime);
                if (hasTime && !readPage.empty()) {
                    lock.lock();
                    try {
                        foregroundWrite(readPage);
                        readPage.clear();
                    } finally {
                        lock.unlock();
                    }
                }
                hasTime = (System.currentTimeMillis() < endTime);
                if (hasTime) {
                    hasTime = drainQueue(readQueue, endTime, false);
                }
                if (hasTime && (diskQueue != null)) {
                    hasTime = drainQueue(diskQueue, endTime, true);
                }
                if (hasTime && !writePage.empty()) {
                    lock.lock();
                    try {
                        foregroundWrite(writePage);
                        writePage.clear();
                    } finally {
                        lock.unlock();
                    }
                }
                unwritten = calculateDirtyPageCount();
                if (unwritten > 0) {
                    log.warn("Closing of disk-backed queue timed out before writing all pages to disk. " +
                         "Approximately {} pages were not written to disk.", unwritten);
                }
                IOException previous = error.get();
                if (previous != null) {
                    closeFuture.completeExceptionally(previous);
                    Throwables.propagate(previous);
                } else {
                    closeFuture.complete(null);
                }
            } catch (IOException ex) {
                closeFuture.completeExceptionally(ex);
                Throwables.propagate(ex);
            } finally {
                synchronized (external) {
                    external.waitForWriteClosure();
                }
            }
        } else {
            try {
                closeFuture.get();
            } catch (InterruptedException|ExecutionException ex) {
                throw Throwables.propagate(ex);
            }
        }
    }

    private int calculateDirtyPageCount() {
        int remaining = (readPage.empty() ? 0 : 1);
        remaining += readQueue.size();
        if (diskQueueSize != null) {
            remaining += diskQueueSize.get();
        }
        if ((readPage != writePage) && !writePage.empty()) {
            remaining += 1;
        }
        return remaining;
    }
}
