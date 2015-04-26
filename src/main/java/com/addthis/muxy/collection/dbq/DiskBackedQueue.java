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

import java.io.Closeable;
import java.io.IOException;

import java.util.concurrent.TimeUnit;

import java.nio.file.Path;
import java.time.Duration;

import com.addthis.muxy.collection.Serializer;

import com.google.common.base.Preconditions;

/**
 * Thread-safe FIFO queue that uses a muxy filesystem to
 * write overflow elements to disk. If memory capacity is not exceeded
 * then all operations occur in memory. {@link #put(Object)}
 * requests are designed to return quickly and write to disk
 * asynchronously if one or more background threads have been configured.
 * All requests of the {@link DiskBackedQueueInternals#get(long, TimeUnit)}
 * family perform synchronous reads from the disk. A {@link #close()}
 * will write the contents of the queue to the muxy filesystem.
 *
 * <p>The class exposes the API of the data structure and the Builder class to construct
 * instances. Nearly all parameter are required to the Builder object.
 * This is a deliberate design decision to discourage a reliance of magical
 * default values that lead to behavior surprises for your application.
 * Refer to {@link DiskBackedQueueInternals} for the
 * the implementation.
 */
public class DiskBackedQueue<E> implements Closeable {

    private final DiskBackedQueueInternals<E> queue;

    private DiskBackedQueue(DiskBackedQueueInternals<E> queue) {
        this.queue = queue;
    }

    public static class Builder<E> {
        private int pageSize = -1;
        private int memMinCapacity = -1;
        private int memMaxCapacity = -1;
        private int diskMaxCapacity = -1;
        private int numBackgroundThreads = -1;
        private Path path;
        private Serializer<E> serializer;
        private Duration terminationWait;
        private Boolean shutdownHook;

        // optional
        private boolean silent;

        /**
         * Number of elements that are stored per page. Larger
         * pages amortize the cost of writing to disk but very large
         * pages limit the concurrency of background writes. Suggested
         * values are in the range from 32 to 4096. This parameter is required.
         */
        public Builder setPageSize(int size) {
            this.pageSize = size;
            return this;
        }

        /**
         * Minimum number of elements that are allowed to be stored
         * in memory. An implementation note: half of these elements
         * will be reserved for the reading queue, and half will be
         * reserved fo the write queue. Suggested values are a 10x to 100x
         * multiple of the page size. The value muse be greater than
         * or equal to page size. This parameter is required.
         */
        public Builder setMemMinCapacity(int capacity) {
            this.memMinCapacity = capacity;
            return this;
        }

        /**
         * Maximum number of elements that are allowed to be stored
         * in memory before insertion operations begin writing
         * synchronously to write to disk. The value muse be greater than
         * or equal to memory minimum capacity. This parameter is required.
         */
        public Builder setMemMaxCapacity(int capacity) {
            this.memMaxCapacity = capacity;
            return this;
        }

        /**
         * Maximum number of elements that are allowed to be stored
         * on disk. Set to 0 to specify no upper bound.
         */
        public Builder setDiskMaxCapacity(int capacity) {
            this.diskMaxCapacity = capacity;
            return this;
        }

        /**
         * Number of background threads for asynchronous writes to disk.
         * If 0 then all writes are synchronous. This parameter is required.
         */
        public Builder setNumBackgroundThreads(int threads) {
            this.numBackgroundThreads = threads;
            return this;
        }

        /**
         * Path to the muxy filesystem. This parameter is required.
         */
        public Builder setPath(Path path) {
            this.path = path;
            return this;
        }

        /**
         * Serializer for reading/writing elements to/from disk.
         * This parameter is required.
         */
        public Builder setSerializer(Serializer<E> serializer) {
            this.serializer = serializer;
            return this;
        }

        /**
         * Time interval to wait for any outstanding writes to be
         * flushed to disk when queue is closed. This field is required.
         */
        public Builder setTerminationWait(Duration wait) {
            this.terminationWait = wait;
            return this;
        }

        /**
         * Whether or not to create a shutdown hook that will
         * close the disk backed queue on JVM shutdown.
         */
        public Builder setShutdownHook(boolean hook) {
            this.shutdownHook = hook;
            return this;
        }

        public Builder setSilent(boolean silence) {
            this.silent = silence;
            return this;
        }

        public DiskBackedQueue<E> build() throws Exception {
            Preconditions.checkArgument(pageSize > 0, "pageSize must be > 0");
            Preconditions.checkArgument(memMinCapacity > 0, "memMinCapacity must be > 0");
            Preconditions.checkArgument(memMaxCapacity > 0, "memMaxCapacity must be > 0");
            Preconditions.checkArgument(diskMaxCapacity >= 0, "diskMaxCapacity must be >= 0");
            Preconditions.checkArgument(numBackgroundThreads >= 0, "numBackgroundThreads must be >= 0");
            Preconditions.checkNotNull(path, "path must be non-null");
            Preconditions.checkNotNull(serializer, "serializer must be non-null");
            Preconditions.checkArgument(memMaxCapacity >= pageSize, "memMaxCapacity must be >= pageSize");
            Preconditions.checkArgument(memMinCapacity <= memMaxCapacity, "memMinCapacity must be <= memMaxCapacity");
            Preconditions.checkNotNull(terminationWait, "terminationWait must be specified");
            Preconditions.checkNotNull(shutdownHook, "shutdownHook usage must be specified");
            return new DiskBackedQueue<>(
                    new DiskBackedQueueInternals<>(pageSize, memMinCapacity / pageSize,
                                                   memMaxCapacity / pageSize,
                                                   diskMaxCapacity / pageSize,
                                                   numBackgroundThreads, path, serializer,
                                                   terminationWait, shutdownHook, silent));
        }
    }

    /**
     * Retrieves and removes the head of this queue,
     * or returns {@code null} if this queue is empty.
     *
     * @return the head of this queue, or {@code null} if this queue is empty
     * @throws IOException if error reading the backing store
     */
    public E poll() throws IOException {
        try {
            return queue.get(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            // This should never happen. The call was nonblocking.
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Retrieves and removes the head of this queue, waiting up to the
     * specified wait time if necessary for an element to become available.
     *
     * @param timeout how long to wait before giving up, in units of
     *        {@code unit}
     * @param unit a {@code TimeUnit} determining how to interpret the
     *        {@code timeout} parameter
     * @return the head of this queue, or {@code null} if the
     *         specified waiting time elapses before an element is available
     * @throws InterruptedException if interrupted while waiting
     * @throws IOException if error reading the backing store
     */
    public E poll(long timeout, TimeUnit unit) throws IOException, InterruptedException {
        Preconditions.checkNotNull(unit);
        return queue.get(timeout, unit);
    }

    /**
     * Retrieves and removes the head of this queue, waiting if necessary
     * until an element becomes available.
     *
     * @return the head of this queue
     * @throws InterruptedException if interrupted while waiting
     * @throws IOException if error reading the backing store
     */
    public E take() throws IOException, InterruptedException {
        return queue.get(0, null);
    }

    /**
     * Inserts the specified element into this queue,
     * waiting if necessary for disk capacity restrictions to be met.
     *
     * @param e the element to add
     * @throws NullPointerException if the specified element is null
     * @throws IllegalArgumentException if some property of the specified
     *         element prevents it from being added to this queue
     * @throws InterruptedException if interrupted while waiting
     * @throws IOException if error reading the backing store
     */
    public void put(E e) throws IOException, InterruptedException {
        queue.offer(e, 0, null);
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
     */
    public boolean offer(E e) throws IOException {
        try {
            return queue.offer(e, 0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            // This should never happen. The call was nonblocking.
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Inserts the specified element into this queue, waiting up to the specified
     * wait time if disk capacity has been exceeded.
     *
     * @param e the element to add
     * @throws NullPointerException if the specified element is null
     * @throws IllegalArgumentException if some property of the specified
     *         element prevents it from being added to this queue
     * @throws InterruptedException if interrupted while waiting
     * @throws IOException if error reading the backing store
     */
    public boolean offer(E e, long timeout, TimeUnit unit) throws IOException, InterruptedException {
        Preconditions.checkNotNull(unit);
        return queue.offer(e, timeout, unit);
    }

    /**
     * Sum of number of pages in memory and number of pages on disk.
     *
     * @return current number of pages.
     */
    public int pageCount() {
        return queue.getPageCount();
    }

    public Path getPath() { return queue.getPath(); }

    @Override
    public void close() {
        queue.close();
    }

    public double fastToSlowWriteRatio() {
        long fastWrite = queue.getFastWrite();
        long slowWrite = queue.getSlowWrite();
        return ((double) fastWrite) / (slowWrite);
    }
}
