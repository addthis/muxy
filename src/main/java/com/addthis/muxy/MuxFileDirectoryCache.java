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
import java.util.concurrent.atomic.AtomicInteger;

import java.nio.file.Path;

import com.addthis.basis.util.JitterClock;
import com.addthis.basis.util.Parameter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages MultiplexFileManager lifecycle by using locks to enforce single
 * instance of MFM per file/directory per machine.
 */
public class MuxFileDirectoryCache {

    private static final Logger log = LoggerFactory.getLogger(MuxFileDirectoryCache.class);

    static int cacheTimer = Parameter.intValue("muxy.cache.timer", 1000);
    static int cacheDirMax = Parameter.intValue("muxy.cache.dir.max", 5);
    static int cacheFileMax = Parameter.intValue("muxy.cache.file.max", 100000);
    static int cacheStreamMax = Parameter.intValue("muxy.cache.stream.max", cacheFileMax);
    static int cacheBytesMax = Parameter.intValue("muxy.cache.bytes.max", MuxDirectory.DEFAULT_BLOCK_SIZE * 3);
    static int writeCacheDirLinger = Parameter.intValue("muxy.cache.dir.lingerWrite", 60000);

    private static final Thread flusher = new Thread() {
        {
            setName("MutiplexedFileServer Cache Flusher");
            setDaemon(true);
            start();
        }

        public void run() {
            while (true) {
                try {
                    sleep(cacheTimer);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    return;
                }
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
        }
    };

    public static boolean tryEvict(MuxFileDirectory muxDir) {
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

    public static boolean tryClear() {
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

    // for managing at-most-once instance of MFM per dir per JVM
    private static final HashMap<Path, TrackedMultiplexFileManager> cache = new HashMap<>();
    private static final AtomicInteger cacheEvictions = new AtomicInteger(0);

    public static int getCacheDirSize() {
        synchronized (cache) {
            return cache.size();
        }
    }

    public static int getAndClearCacheEvictions() {
        return cacheEvictions.getAndSet(0);
    }

    public static long getCacheByteSize() {
        synchronized (cache) {
            long size = 0;
            for (TrackedMultiplexFileManager mfm : cache.values()) {
                size += mfm.prevBytes;
            }
            return size;
        }
    }

    public static int getCacheStreamSize() {
        synchronized (cache) {
            int size = 0;
            for (MuxFileDirectory mfm : cache.values()) {
                size += mfm.writeStreamMux.size();
            }
            return size;
        }
    }

    public static int getCacheFileSize() {
        synchronized (cache) {
            int size = 0;
            for (MuxFileDirectory mfm : cache.values()) {
                size += mfm.getFileCount();
            }
            return size;
        }
    }

    /* returns an authoritative instance of an MFM for a given directory */
    private static TrackedMultiplexFileManager getAuthoritativeInstance(File dir) throws Exception {
        return getAuthoritativeInstance(dir.toPath());
    }

    /* returns an authoritative instance of an MFM for a given directory */
    private static TrackedMultiplexFileManager getAuthoritativeInstance(Path dir) throws Exception {
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
            mfm.releaseAfter(writeCacheDirLinger);
            return mfm;
        }
    }

    public static void waitForWriteClosure() {
        MuxFileDirectory list[] = null;
        synchronized (cache) {
            list = cache.values().toArray(new MuxFileDirectory[cache.size()]);
        }
        for (MuxFileDirectory mfm : list) {
            mfm.waitForWriteClosure();
        }
    }

    /* open and cache for min of 60 seconds */
    public static TrackedMultiplexFileManager getWriteableInstance(File dir) throws Exception {
        TrackedMultiplexFileManager mfm = getAuthoritativeInstance(dir);
        return mfm;
    }

    /* tracks usage */
    public static final class TrackedMultiplexFileManager extends MuxFileDirectory {

        private long releaseTime;
        private long prevBytes;

        private TrackedMultiplexFileManager(Path dir, TrackedFileEventListener listener) throws Exception {
            super(dir, listener);
            listener.setTrackedInstance(this);
        }

        public void releaseAfter(long time) {
            releaseTime = Math.max(JitterClock.globalTime() + time, releaseTime);
        }

        public boolean checkRelease() {
            return releaseTime > 0 && releaseTime < JitterClock.globalTime();
        }
    }

    /* tracks usage events */
    private static class TrackedFileEventListener implements MuxyFileEventListener {

        private TrackedMultiplexFileManager mfm;

        @Override
        public void event(MuxyFileEvent event, Object target) {
            if (event == MuxyFileEvent.LOG_COMPACT) {
                mfm.releaseAfter(1000);
            }
        }

        private void setTrackedInstance(TrackedMultiplexFileManager mfm) {
            this.mfm = mfm;
        }
    }
}
