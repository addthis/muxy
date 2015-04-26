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
package com.addthis.muxy.collection.dbq;

import javax.annotation.concurrent.GuardedBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import java.nio.file.Path;
import java.time.Duration;

import com.addthis.muxy.MuxFile;
import com.addthis.muxy.MuxFileDirectory;
import com.addthis.muxy.MuxyEventListener;
import com.addthis.muxy.MuxyFileEvent;
import com.addthis.muxy.MuxyStreamEvent;
import com.addthis.muxy.WritableMuxFile;
import com.addthis.muxy.collection.Serializer;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DiskBackedQueueInternals<E> implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(DiskBackedQueue.class);

    private final int pageSize;

    private final int minReadPages;

    private final int minWritePages;

    private final int maxPages;

    private final int maxDiskPages;

    private final Serializer<E> serializer;

    private final Duration terminationWait;

    private final boolean silent;

    private final ReentrantLock lock = new ReentrantLock();

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
     * Wakes up any readers that are waiting for writers.
     * Is signalled whenever an element is inserted
     * into the queue or when a page is read
     * from disk.
     */
    @GuardedBy("lock")
    private final Condition notEmpty = lock.newCondition();

    /**
     * Defined as {@code lock.newCondition()}.
     * Wakes up any writers that are waiting for readers
     * to catch up. Is signalled whenever the readPage reference
     * is updated.
     */
    @GuardedBy("lock")
    private final Condition notFull = lock.newCondition();

    /**
     * Pages waiting to be evicted to disk. Any page
     * in the disk queue must not referenced by the
     * {@code writePage} or the {@code readPage}.
     */
    private final ConcurrentSkipListMap<Long, Page<E>> diskQueue;

    /**
     * Estimate the current size of the diskQueue.
     */
    private final AtomicInteger diskQueueSize;

    /**
     * Pages pulled from the disk and waiting to
     * placed into the {@code readPage}.
     */
    @GuardedBy("lock")
    private final NavigableMap<Long, Page<E>> readQueue = new TreeMap<>();

    @GuardedBy("lock")
    private Page<E> writePage;

    @GuardedBy("lock")
    private Page<E> readPage;

    private final AtomicReference<IOException> error = new AtomicReference<>(null);

    private final ScheduledExecutorService backgroundTasks;

    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    private final AtomicBoolean closed = new AtomicBoolean();

    private final AtomicInteger pageCount = new AtomicInteger();

    private final AtomicLong fastWrite = new AtomicLong();

    private final AtomicLong slowWrite = new AtomicLong();

    /**
     * To prevent a thundering herd of page loads only a single
     * thread is permitted to load at any time.
     */
    private final Semaphore loadPageSemaphore = new Semaphore(1);

    /**
     * Use {@link DiskBackedQueue.Builder} to construct a disk-backed queue.
     * Throws an exception the mux directory cannot be created or opened.
     * Throws an exception if {@code shutdownHook} is true and the JVM is
     * currently shutting down.
     */
    DiskBackedQueueInternals(int pageSize, int minPages, int maxPages, int maxDiskPages,
                             int numBackgroundThreads, Path path, Serializer<E> serializer,
                             Duration terminationWait, boolean shutdownHook, boolean silent) throws Exception {
        this.pageSize = pageSize;
        this.maxPages = maxPages;
        this.maxDiskPages = maxDiskPages;
        this.external = new MuxFileDirectory(path, new NextReadPageListener());
        this.serializer = serializer;
        this.terminationWait = terminationWait;
        this.silent = silent;
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
                    numBackgroundThreads,
                    new ThreadFactoryBuilder().setNameFormat("disk-backed-queue-writer-%d").build());
            this.minReadPages = Math.max(minPages / 2, 1); // always fetch at least one page
            this.minWritePages = minPages / 2;
            for (int i = 0; i < numBackgroundThreads; i++) {
                backgroundTasks.scheduleWithFixedDelay(new DiskWriteTask(),
                                                       0, 10, TimeUnit.MILLISECONDS);
            }
        }
        Collection<MuxFile> files = external.listFiles();
        Optional<MuxFile> minFile = files.stream().min((f1, f2) -> (f1.getName().compareTo(f2.getName())));
        Optional<MuxFile> maxFile = files.stream().max((f1, f2) -> (f2.getName().compareTo(f1.getName())));
        if (minFile.isPresent() && maxFile.isPresent()) {
            long readPageId, writePageId;
            readPageId  = Long.parseLong(minFile.get().getName());
            writePageId = Long.parseLong(maxFile.get().getName()) + 1;
            pageCount.set((int) (writePageId - readPageId + 1));
            NavigableMap<Long, Page<E>> readPages = readPagesFromExternal(readPageId, minReadPages);
            readPage = readPages.remove(readPageId);
            readQueue.putAll(readPages);
            writePage = new Page<>(writePageId, pageSize, serializer, external);
        } else {
            writePage = new Page<>(0, pageSize, serializer, external);
            readPage = writePage;
            pageCount.set(1);
        }
        if (shutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread(this::close, "disk-backed-queue-shutdown"));
        }
    }

    private boolean drainQueue(NavigableMap<Long,Page<E>> queue, long endTime, boolean diskQueue) throws IOException {
        if ((endTime > 0) && (System.currentTimeMillis() >= endTime)) {
            return false;
        }
        Map.Entry<Long,Page<E>> minEntry;
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

    private NavigableMap<Long,Page<E>> readPagesFromExternal(long id, int count) throws IOException {
        NavigableMap<Long,Page<E>> results = new TreeMap<>();
        for(long i = id; i < (id + count); i++) {
            ByteArrayOutputStream copyStream = new ByteArrayOutputStream();
            synchronized (external) {
                if (!external.exists(Long.toString(i))) {
                    return results;
                }
                WritableMuxFile file = external.openFile(Long.toString(i), false);
                try (InputStream input = file.read()) {
                    ByteStreams.copy(input, copyStream);
                }
                file.delete();
            }
            Page<E> page = new Page<>(i, pageSize, serializer, external,
                                      new ByteArrayInputStream(copyStream.toByteArray()));
            results.put(i, page);
        }
        return results;
    }

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
        NavigableMap<Long,Page<E>> loadedPages;
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
            pageCount.set(1);
            notFull.signalAll();
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
    private boolean fetchFromQueue(NavigableMap<Long,Page<E>> queue, long id, boolean diskQueue) {
        assert(lock.isHeldByCurrentThread());
        assert(readPage.empty());
        assert(id == (readPage.id + 1));
        Page<E> nextPage = queue.remove(id);
        if (nextPage != null) {
            if (diskQueue) {
                diskQueueSize.decrementAndGet();
            }
            readPage = nextPage;
            pageCount.decrementAndGet();
            notEmpty.signalAll();
            notFull.signalAll();
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
            throw new IllegalStateException("attempted read after close()");
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
                if (closed.get()) {
                    throw new IllegalStateException("read did not complete before close()");
                }
                long nextId = readPage.id + 1;
                if (!readPage.empty()) {
                    return readPage.remove();
                } else if (fetchFromQueues(nextId)) {
                    return readPage.remove();
                } else if ((nextId == writePage.id) && (readPage != writePage)) {
                    readPage = writePage;
                    pageCount.set(1);
                    notFull.signalAll();
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

    private void testNotEmpty() {
        // signal if the new element is the next in FIFO order for the consumers
        if ((((readPage.id + 1) == writePage.id) && readPage.empty()) ||
            ((readPage == writePage) && (writePage.count == 1))) {
            notEmpty.signal();
        }
    }

    /**
     * Inserts the specified element into this queue if it is possible
     * to do so without violating disk capacity restrictions.
     *
     * @param e the element to add
     * @throws NullPointerException if the specified element is null
     * @throws IllegalArgumentException if some property of the specified
     *         element prevents it from being added to this queue
     * @throws IOException if error reading the backing store
     * @throws InterruptedException
     */
    boolean offer(E e, long timeout, TimeUnit unit) throws IOException, InterruptedException {
        if (closed.get()) {
            throw new IllegalStateException("attempted write after close()");
        }
        Preconditions.checkNotNull(e);
        propagateError();
        /**
         * Follow example in
         * https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/locks/Condition.html#awaitNanos-long-
         * to handle spurious wakeups.
         */
        long nanos = (unit == null) ? 0 : unit.toNanos(timeout);
        lock.lock();
        try {
            while(true) {
                if (closed.get()) {
                    throw new IllegalStateException("write did not complete before close()");
                }
                if (!writePage.full()) {
                    writePage.add(e);
                    testNotEmpty();
                    fastWrite.getAndIncrement();
                    return true;
                } else if ((maxDiskPages > 0) && ((writePage.id - readPage.id) >= maxDiskPages)) {
                    if (unit == null) {
                        notFull.await();
                    } else if (nanos <= 0L) {
                        return false;
                    } else {
                        nanos = notFull.awaitNanos(nanos);
                    }
                } else {
                    Page<E> oldPage = writePage;
                    writePage = new Page<>(oldPage.id + 1, pageSize, serializer, external);
                    writePage.add(e);
                    pageCount.incrementAndGet();
                    if (readPage != oldPage) {
                        if ((diskQueue == null) || (diskQueueSize.get() > maxPages)) {
                            foregroundWrite(oldPage);
                            slowWrite.getAndIncrement();
                        } else {
                            Page previous = diskQueue.put(oldPage.id, oldPage);
                            assert (previous == null);
                            diskQueueSize.incrementAndGet();
                            fastWrite.getAndIncrement();
                        }
                    }
                    testNotEmpty();
                    return true;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void foregroundWrite(Page<E> page) throws IOException {
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
                        if (!silent) {
                            log.info("Waiting on background threads to write approximately {} pages",
                                     diskQueueSize.get());
                        }
                        backgroundTasks.awaitTermination(terminationWait.toMillis(), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException ex) {
                        closeFuture.completeExceptionally(ex);
                        Throwables.propagate(ex);
                    } finally {
                        backgroundTasks.shutdownNow();
                    }
                }
                int unwritten = calculateDirtyPageCount();
                if (!silent) {
                    log.info("Foreground thread must write approximately {} pages", unwritten);
                }
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

    public int getPageCount() {
        return pageCount.get();
    }

    public Path getPath() { return external.getDirectory(); }

    public long getFastWrite() { return fastWrite.get(); }

    public long getSlowWrite() { return slowWrite.get(); }

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

    private class DiskWriteTask implements Runnable {

        @Override public void run() {
            while ((getError() == null) && (diskQueueSize.get() > minWritePages)) {
                try {
                    Map.Entry<Long,Page<E>> minEntry = diskQueue.pollFirstEntry();
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

}
