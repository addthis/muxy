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

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import java.nio.file.Path;

import com.addthis.basis.util.Parameter;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages MultiplexFileManager lifecycle by using locks to enforce single
 * instance of MFM per file/directory per machine.
 */
class MuxFileDirectoryCacheInstance {

    private static final Logger log = LoggerFactory.getLogger(MuxFileDirectoryCacheInstance.class);

    static final int CACHE_TIMER = Parameter.intValue("muxy.cache.timer", 1000);
    static final int CACHE_DIR_MAX = Parameter.intValue("muxy.cache.dir.max", 5);
    static final int CACHE_FILE_MAX = Parameter.intValue("muxy.cache.file.max", 100000);
    static final int CACHE_STREAM_MAX = Parameter.intValue("muxy.cache.stream.max", CACHE_FILE_MAX);
    static final int CACHE_BYTES_MAX = Parameter.intValue("muxy.cache.bytes.max", MuxDirectory.DEFAULT_BLOCK_SIZE * 3);
    static final int WRITE_CACHE_DIR_LINGER = Parameter.intValue("muxy.cache.dir.lingerWrite", 60000);

    final int cacheTimer;
    final int cacheDirMax;
    final int cacheFileMax;
    final int cacheStreamMax;
    final int cacheBytesMax;
    final int writeCacheDirLiner;

    private final HashMap<Path, TrackedMultiplexFileManager> cache = new HashMap<>();
    private final AtomicInteger cacheEvictions = new AtomicInteger(0);

    private final ScheduledExecutorService writableDirectoryCacheEvictor = MoreExecutors
            .getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1,
                    new ThreadFactoryBuilder().setNameFormat("muxyDirectoryCacheEvictor=%d").build()));

    public MuxFileDirectoryCacheInstance(int cacheTimer, int cacheDirMax, int cacheFileMax, int cacheStreamMax, int cacheBytesMax, int writeCacheDirLiner) {
        this.cacheTimer = cacheTimer;
        this.cacheDirMax = cacheDirMax;
        this.cacheFileMax = cacheFileMax;
        this.cacheStreamMax = cacheStreamMax;
        this.cacheBytesMax = cacheBytesMax;
        this.writeCacheDirLiner = writeCacheDirLiner;
        if (cacheTimer > 0) {
            writableDirectoryCacheEvictor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    doEviction();
                }
            }, cacheTimer, cacheTimer, TimeUnit.MILLISECONDS);
        }
    }

    private MuxFileDirectoryCacheInstance(Builder builder) {
        this(builder.cacheTimer, builder.cacheDirMax, builder.cacheFileMax,
                builder.cacheStreamMax, builder.cacheBytesMax, builder.writeCacheDirLiner);
    }

    private void doEviction() {
        synchronized (cache) {
            TrackedMultiplexFileManager tmfm[] = cache.values().toArray(new TrackedMultiplexFileManager[cache.size()]);
            Arrays.sort(tmfm, new Comparator<TrackedMultiplexFileManager>() {
                @Override
                public int compare(TrackedMultiplexFileManager o1, TrackedMultiplexFileManager o2) {
                    return (int) (o1.releaseTime - o2.releaseTime);
                }
            });
            long cachedStreams = getCacheStreamSize();
            long cachedBytes = getCacheByteSize();
            for (TrackedMultiplexFileManager mfm : tmfm) {
                long currentBytes = mfm.writeStreamMux.openWriteBytes.get();
                if ((cache.size() > cacheDirMax || cachedStreams > cacheStreamMax)
                    && mfm.checkRelease() && mfm.waitForWriteClosure(0)) {
                    cache.remove(mfm.getDirectory());
                    cachedStreams -= mfm.writeStreamMux.size();
                    cacheEvictions.incrementAndGet();
                    if (log.isDebugEnabled()) {
                        log.debug("flush.ok " + mfm.getDirectory() + " files=" + mfm.getFileCount() + " complete=" + mfm.isWritingComplete());
                    }
                    cachedBytes -= currentBytes; //not as accurate as the return from wSTB but fine
                } else {
                    if (cachedBytes > cacheBytesMax && currentBytes != 0 && currentBytes == mfm.prevBytes) {
                        try {
                            cachedBytes -= mfm.writeStreamMux.writeStreamsToBlock();
                        } catch (IOException ex) {
                            log.error("IOException while calling write streams to block", ex);
                        }
                        mfm.prevBytes = 0; //perhaps not true but fine
                    } else if (currentBytes == 0 && mfm.prevBytes == 0) {
                        mfm.writeStreamMux.trimOutputBuffers();
                    } else {
                        mfm.prevBytes = currentBytes;
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("flush.skip " + mfm.getDirectory() + " files=" + mfm.getFileCount() + " complete=" + mfm.isWritingComplete());
                    }
                }
            }
            if (cachedBytes > cacheBytesMax) //if we are still over the max, then ignore the equality heuristic
            {
                tmfm = cache.values().toArray(new TrackedMultiplexFileManager[cache.size()]);
                Arrays.sort(tmfm, new Comparator<TrackedMultiplexFileManager>() //sort largest first
                {
                    @Override
                    public int compare(TrackedMultiplexFileManager o1, TrackedMultiplexFileManager o2) {
                        return (int) (o2.prevBytes - o1.prevBytes); //reversed
                    }
                });
                for (TrackedMultiplexFileManager mfm : tmfm) {
                    if (cachedBytes > cacheBytesMax) {
                        try {
                            cachedBytes -= mfm.writeStreamMux.writeStreamsToBlock();
                        } catch (IOException ex) {
                            log.error("IOException while calling write streams to block", ex);
                        }
                        mfm.prevBytes = 0; //perhaps not true but fine
                    } else {
                        break;
                    }
                }
            }
        }
    }

    public boolean tryEvict(MuxFileDirectory muxDir) {
        synchronized (cache) {
            TrackedMultiplexFileManager tmfm[] = cache.values().toArray(new TrackedMultiplexFileManager[cache.size()]);
            for (TrackedMultiplexFileManager mfm : tmfm) {
                if (mfm == muxDir) {
                    muxDir.waitForWriteClosure(0);
                    cache.remove(mfm.getDirectory());
                    cacheEvictions.incrementAndGet();
                    return true;
                }
            }
        }
        return false;
    }

    public boolean tryClear() {
        synchronized (cache) {
            TrackedMultiplexFileManager tmfm[] = cache.values().toArray(new TrackedMultiplexFileManager[cache.size()]);
            for (TrackedMultiplexFileManager mfm : tmfm) {
                if (mfm.waitForWriteClosure(0)) {
                    cache.remove(mfm.getDirectory());
                    cacheEvictions.incrementAndGet();
                }
            }
            return getCacheDirSize() == 0;
        }
    }

    public int getCacheDirSize() {
        synchronized (cache) {
            return cache.size();
        }
    }

    public int getAndClearCacheEvictions() {
        return cacheEvictions.getAndSet(0);
    }

    public long getCacheByteSize() {
        synchronized (cache) {
            long size = 0;
            for (TrackedMultiplexFileManager mfm : cache.values()) {
                size += mfm.prevBytes;
            }
            return size;
        }
    }

    public int getCacheStreamSize() {
        synchronized (cache) {
            int size = 0;
            for (MuxFileDirectory mfm : cache.values()) {
                size += mfm.writeStreamMux.size();
            }
            return size;
        }
    }

    public int getCacheFileSize() {
        synchronized (cache) {
            int size = 0;
            for (MuxFileDirectory mfm : cache.values()) {
                size += mfm.getFileCount();
            }
            return size;
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
        synchronized (cache) {
            final Path realPath = dir.toRealPath();
            TrackedMultiplexFileManager mfm = cache.get(realPath);
            if (mfm == null) {
                mfm = new TrackedMultiplexFileManager(realPath, new TrackedFileEventListener());
                cache.put(realPath, mfm);
            }
            mfm.releaseAfter(writeCacheDirLiner);
            return mfm;
        }
    }

    public void waitForWriteClosure() {
        MuxFileDirectory list[] = null;
        synchronized (cache) {
            list = cache.values().toArray(new MuxFileDirectory[cache.size()]);
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

    public static final class Builder {

        private int cacheTimer = CACHE_TIMER;
        private int cacheDirMax = CACHE_DIR_MAX;
        private int cacheFileMax = CACHE_FILE_MAX;
        private int cacheStreamMax = CACHE_STREAM_MAX;
        private int cacheBytesMax = CACHE_BYTES_MAX;
        private int writeCacheDirLiner = WRITE_CACHE_DIR_LINGER;

        public Builder() {
        }

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
