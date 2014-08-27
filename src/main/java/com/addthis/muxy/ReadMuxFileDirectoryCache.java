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

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import java.nio.file.Path;

import com.addthis.basis.util.Parameter;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read Only Version of MFS. Uses a guava cache. This is primarily to help with concurrency issues.
 * In particular, we do not want to hold a lock on the whole cache while inflating a muxy directory.
 * That involves disk i/o and can easily block when multiple threads are trying to open a lot of
 * directories (as often happens with eg. meshy).
 */
public class ReadMuxFileDirectoryCache {

    private static final Logger log = LoggerFactory.getLogger(ReadMuxFileDirectoryCache.class);

    //In guava parlance this will be size = 1.
    private static final int cacheDirMax = Parameter.intValue("muxy.cache.dir.max", 100);
    //And this will use weights where file count = weight
    private static final int cacheFileMax = Parameter.intValue("muxy.cache.file.max", 100000);
    //And this will use weights where stream count = weight. There is always at least one
    // stream per file and perhaps arbitrarily more
    private static final int cacheStreamMax = Parameter.intValue("muxy.cache.stream.max", cacheFileMax);

    private static final boolean useStreamMax = Parameter.boolValue("muxy.cache.useStreamMax", true);

    static final Gauge<Integer> cacheDirSize = Metrics.newGauge(ReadMuxFileDirectoryCache.class, "directory-cache", "directories", new Gauge<Integer>() {
        @Override
        public Integer value() {
            return getCacheDirSize();
        }
    });
    static final Gauge<Integer> cacheFileSize = Metrics.newGauge(ReadMuxFileDirectoryCache.class, "directory-cache", "total-files", new Gauge<Integer>() {
        @Override
        public Integer value() {
            return getCacheFileSize();
        }
    });
    static final Gauge<Integer> cacheStreamSize = Metrics.newGauge(ReadMuxFileDirectoryCache.class, "directory-cache", "total-streams", new Gauge<Integer>() {
        @Override
        public Integer value() {
            return getCacheStreamSize();
        }
    });

    private static final LoadingCache<Path, ReadMuxFileDirectory> loadingMuxDirCache = CacheBuilder.newBuilder()
            .maximumWeight(useStreamMax ? cacheStreamMax : cacheDirMax)
            .refreshAfterWrite(30000, TimeUnit.MILLISECONDS)
            .weigher(
                    new Weigher<Path, ReadMuxFileDirectory>() {
                        public int weigh(Path key, ReadMuxFileDirectory mfm) {
                            if (useStreamMax) {
                                return mfm.streamMux.size() + 1;
                            } else {
                                return 1;
                            }
                        }
                    }
            )
            .build(
                    new ReadMuxFileDirectoryCacheLoader()
            );

    public static int getCacheDirSize() {
        //return the guava cache size
        return (int) loadingMuxDirCache.size();
    }

    public static int getCacheStreamSize() {
        //return the guava cache size
        int size = 0;
        for (ReadMuxFileDirectory mfm : loadingMuxDirCache.asMap().values()) {
            size += mfm.streamMux.size();
        }
        return size;
    }

    public static int getCacheFileSize() {
        int size = 0;
        for (ReadMuxFileDirectory mfm : loadingMuxDirCache.asMap().values()) {
            size += mfm.getFileCount();
        }
        return size;
    }

    /**
     * Handles null in the same was as normal MFServer and does canonical path resolving.
     */
    static ReadMuxFileDirectory getResolvedInstance(File dir) throws Exception {
        if (dir == null) {
            dir = new File(".");
        }
        final Path path = dir.toPath();
        return loadingMuxDirCache.get(path);
    }

    // These are all shortcuts / macros

    /* special case for StreamServer/Meshy that macros in listFiles */
    public static Collection<ReadMuxFile> listFiles(File dir) throws Exception {
        return getResolvedInstance(dir).listFiles();
    }

    /* special case for StreamServer/Meshy that opens a specific muxed file */
    public static ReadMuxFile getFileMeta(File dir, String name) throws Exception {
        return getResolvedInstance(dir).openFile(name, false);
    }

    /* special case for StreamServer/Meshy that reads from a specific muxed file */
    public static InputStream readFile(File dir, String name, long offset) throws Exception {
        return getFileMeta(dir, name).read(offset);
    }

}
