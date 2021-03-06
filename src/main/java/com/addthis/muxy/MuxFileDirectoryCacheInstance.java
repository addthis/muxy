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

import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import java.nio.file.Path;

import com.addthis.basis.util.Parameter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages MultiplexFileManager lifecycle by using locks to enforce single
 * instance of MFM per file/directory per machine.
 */
class MuxFileDirectoryCacheInstance implements WriteTracker {

    private static final Logger log = LoggerFactory.getLogger(MuxFileDirectoryCacheInstance.class);

    static final int CACHE_TIMER = Parameter.intValue("muxy.cache.timer", 1000);
    static final int CACHE_DIR_MAX = Parameter.intValue("muxy.cache.dir.max", 15);
    static final int CACHE_FILE_MAX = Parameter.intValue("muxy.cache.file.max", 100000);
    static final int CACHE_STREAM_MAX = Parameter.intValue("muxy.cache.stream.max", CACHE_FILE_MAX);
    static final int CACHE_BYTES_MAX = Parameter.intValue("muxy.cache.bytes.max", MuxDirectory.DEFAULT_BLOCK_SIZE * 2);
    static final int WRITE_CACHE_DIR_LINGER = Parameter.intValue("muxy.cache.dir.lingerWrite", 60000);

    final int cacheTimer;
    final int cacheDirMax;
    final int cacheFileMax;
    final long cacheStreamMax;
    final long cacheBytesMax;
    final int writeCacheDirLiner;

    private final Lock cacheLock = new ReentrantLock();
    @GuardedBy("cacheLock") private final Map<Path, TrackedMultiplexFileManager> cache;

    private final AtomicInteger cacheEvictions = new AtomicInteger(0);
    private final AtomicLong openWriteBytes = new AtomicLong(0);
    private final AtomicLong streamCount = new AtomicLong(0);

    public MuxFileDirectoryCacheInstance(int cacheTimer,
                                         int cacheDirMax,
                                         int cacheFileMax,
                                         long cacheStreamMax,
                                         long cacheBytesMax,
                                         int writeCacheDirLiner) {
        this.cacheTimer = cacheTimer;
        this.cacheDirMax = cacheDirMax;
        this.cacheFileMax = cacheFileMax;
        this.cacheStreamMax = cacheStreamMax;
        this.cacheBytesMax = cacheBytesMax;
        this.writeCacheDirLiner = writeCacheDirLiner;
        this.cache = new HashMap<>(cacheDirMax);
    }

    private MuxFileDirectoryCacheInstance(Builder builder) {
        this(builder.cacheTimer, builder.cacheDirMax, builder.cacheFileMax,
             builder.cacheStreamMax, builder.cacheBytesMax, builder.writeCacheDirLiner);
    }

    private void doEviction() {
        cacheLock.lock();
        try {
            TrackedMultiplexFileManager[] tmfm = cache.values().toArray(new TrackedMultiplexFileManager[cache.size()]);
            Arrays.sort(tmfm, new Comparator<TrackedMultiplexFileManager>() {
                @Override
                public int compare(TrackedMultiplexFileManager o1, TrackedMultiplexFileManager o2) {
                    return Long.compare(o1.releaseTime, o2.releaseTime);
                }
            });
            long cachedStreams = getCacheStreamSize();
            long cachedBytes = getCacheByteSize();
            for (TrackedMultiplexFileManager mfm : tmfm) {
                long currentBytes = mfm.getStreamManager().openWriteBytes.get();
                if (((cache.size() > cacheDirMax) || (cachedStreams > cacheStreamMax))
                    && mfm.checkRelease()
                    && mfm.waitForWriteClosure(0)) {
                    cache.remove(mfm.getDirectory());
                    cachedStreams -= mfm.getStreamManager().size();
                    streamCount.addAndGet(-mfm.getStreamManager().size());
                    cacheEvictions.incrementAndGet();
                    if (log.isDebugEnabled()) { // non-trivial arg cost
                        log.debug("flush.ok {} files={} complete={}",
                                  mfm.getDirectory(), mfm.getFileCount(), mfm.isWritingComplete());
                    }
                    cachedBytes -= currentBytes; //not as accurate as the return from wSTB but fine
                } else {
                    if ((cachedBytes > cacheBytesMax) && (currentBytes != 0) && (currentBytes == mfm.prevBytes)) {
                        try {
                            cachedBytes -= mfm.getStreamManager().writeStreamsToBlock();
                        } catch (IOException ex) {
                            log.error("IOException while calling write streams to block", ex);
                        }
                        mfm.prevBytes = 0; //perhaps not true but fine
                    } else if ((currentBytes == 0) && (mfm.prevBytes == 0)) {
                        mfm.getStreamManager().maybeTrimOutputBuffers();
                    } else {
                        mfm.prevBytes = currentBytes;
                    }
                    if (log.isDebugEnabled()) { // non-trivial arg cost
                        log.debug("flush.skip {} files={} complete={}",
                                  mfm.getDirectory(), mfm.getFileCount(), mfm.isWritingComplete());
                    }
                }
            }
            //if we are still over the max, then ignore the equality heuristic. we check getCacheByteSize()
            //  again just to make an edge case where a huge write was done by someone else less likely to
            //  lead to needlessly small writes.
            if ((cachedBytes > cacheBytesMax) && (getCacheByteSize() > cacheBytesMax)) {
                tmfm = cache.values().toArray(new TrackedMultiplexFileManager[cache.size()]);
                Arrays.sort(tmfm, new Comparator<TrackedMultiplexFileManager>() //sort largest first
                {
                    @Override
                    public int compare(TrackedMultiplexFileManager o1, TrackedMultiplexFileManager o2) {
                        return Long.compare(o2.prevBytes,  o1.prevBytes); //intentionally reversed
                    }
                });
                for (TrackedMultiplexFileManager mfm : tmfm) {
                    if (cachedBytes > cacheBytesMax) {
                        try {
                            cachedBytes -= mfm.getStreamManager().writeStreamsToBlock();
                        } catch (IOException ex) {
                            log.error("IOException while calling write streams to block", ex);
                        }
                        mfm.prevBytes = 0; //perhaps not true but fine
                    } else {
                        break;
                    }
                }
            }
        } finally {
            cacheLock.unlock();
        }
    }

    public boolean tryEvict(MuxFileDirectory muxDir) {
        cacheLock.lock();
        try {
            if (cache.containsKey(muxDir.getDirectory())) {
                muxDir.waitForWriteClosure(0);
                cache.remove(muxDir.getDirectory());
                streamCount.addAndGet(-muxDir.getStreamManager().size());
                cacheEvictions.incrementAndGet();
                return true;
            }
        } finally {
            cacheLock.unlock();
        }
        return false;
    }

    public boolean tryClear() {
        cacheLock.lock();
        try {
            TrackedMultiplexFileManager[] tmfm = cache.values().toArray(new TrackedMultiplexFileManager[cache.size()]);
            for (TrackedMultiplexFileManager mfm : tmfm) {
                if (mfm.waitForWriteClosure(0)) {
                    cache.remove(mfm.getDirectory());
                    streamCount.addAndGet(-mfm.getStreamManager().size());
                    cacheEvictions.incrementAndGet();
                }
            }
            return getCacheDirSize() == 0;
        } finally {
            cacheLock.unlock();
        }
    }

    public int getCacheDirSize() {
        cacheLock.lock();
        try {
            return cache.size();
        } finally {
            cacheLock.unlock();
        }
    }

    public int getAndClearCacheEvictions() {
        return cacheEvictions.getAndSet(0);
    }

    public long getCacheByteSize() {
        return openWriteBytes.get();
    }

    public long getCacheStreamSize() {
        return streamCount.get();
    }

    public int getCacheFileSize() {
        cacheLock.lock();
        try {
            int size = 0;
            for (MuxFileDirectory mfm : cache.values()) {
                size += mfm.getFileCount();
            }
            return size;
        } finally {
            cacheLock.unlock();
        }
    }

    /* returns an authoritative instance of an MFM for a given directory */
    public TrackedMultiplexFileManager getAuthoritativeInstance(File dir) throws Exception {
        return getAuthoritativeInstance(dir.toPath());
    }

    /* returns an authoritative instance of an MFM for a given directory */
    public TrackedMultiplexFileManager getAuthoritativeInstance(Path dir) throws Exception {
        if (dir == null) {
            return null;
        }
        cacheLock.lock();
        try {
            final Path realPath = dir.toRealPath();
            TrackedMultiplexFileManager mfm = cache.get(realPath);
            if (mfm == null) {
                mfm = new TrackedMultiplexFileManager(realPath, new TrackedFileEventListener(), this);
                cache.put(realPath, mfm);
                reportStreams((long) mfm.getStreamManager().size());
                if (cache.size() > cacheDirMax) {
                    doEviction();
                }
            }
            mfm.releaseAfter(writeCacheDirLiner);
            return mfm;
        } finally {
            cacheLock.unlock();
        }
    }

    public void waitForWriteClosure() {
        MuxFileDirectory[] list = null;
        cacheLock.lock();
        try {
            list = cache.values().toArray(new MuxFileDirectory[cache.size()]);
        } finally {
            cacheLock.unlock();
        }
        for (MuxFileDirectory mfm : list) {
            mfm.waitForWriteClosure();
        }
    }

    /* open and cache for min of 60 seconds */
    public TrackedMultiplexFileManager getWriteableInstance(File dir) throws Exception {
        TrackedMultiplexFileManager mfm = getAuthoritativeInstance(dir);
        return mfm;
    }

    @Override
    public void reportWrite(long bytes) {
        long newPendingBytes = openWriteBytes.addAndGet(bytes);
        if ((bytes > 0) && (newPendingBytes > cacheBytesMax)) {
            doEviction();
        }
    }

    @Override
    public void reportStreams(long streams) {
        long newStreamCount = streamCount.addAndGet(streams);
        if ((streams > 0) && (newStreamCount > cacheStreamMax)) {
            doEviction();
        }
    }

    public static final class Builder {
        private int cacheTimer = CACHE_TIMER;
        private int cacheDirMax = CACHE_DIR_MAX;
        private int cacheFileMax = CACHE_FILE_MAX;
        private long cacheStreamMax = CACHE_STREAM_MAX;
        private long cacheBytesMax = CACHE_BYTES_MAX;
        private int writeCacheDirLiner = WRITE_CACHE_DIR_LINGER;

        public Builder() {}

        public Builder(MuxFileDirectoryCacheInstance copy) {
            cacheTimer = copy.cacheTimer;
            cacheDirMax = copy.cacheDirMax;
            cacheFileMax = copy.cacheFileMax;
            cacheStreamMax = copy.cacheStreamMax;
            cacheBytesMax = copy.cacheBytesMax;
            writeCacheDirLiner = copy.writeCacheDirLiner;
        }

        public Builder cacheTimer(int cacheTimer) {
            this.cacheTimer = cacheTimer;
            return this;
        }

        public Builder cacheDirMax(int cacheDirMax) {
            this.cacheDirMax = cacheDirMax;
            return this;
        }

        public Builder cacheFileMax(int cacheFileMax) {
            this.cacheFileMax = cacheFileMax;
            return this;
        }

        public Builder cacheStreamMax(int cacheStreamMax) {
            this.cacheStreamMax = cacheStreamMax;
            return this;
        }

        public Builder cacheBytesMax(int cacheBytesMax) {
            this.cacheBytesMax = cacheBytesMax;
            return this;
        }

        public Builder writeCacheDirLiner(int writeCacheDirLiner) {
            this.writeCacheDirLiner = writeCacheDirLiner;
            return this;
        }

        public MuxFileDirectoryCacheInstance build() {
            return new MuxFileDirectoryCacheInstance(this);
        }
    }
}
