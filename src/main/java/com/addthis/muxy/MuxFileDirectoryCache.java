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

import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages MultiplexFileManager lifecycle by using locks to enforce single
 * instance of MFM per file/directory per machine.
 */
public class MuxFileDirectoryCache {

    private static final Logger log = LoggerFactory.getLogger(MuxFileDirectoryCache.class);

    // for managing at-most-once instance of MFM per dir per JVM
    public static final MuxFileDirectoryCacheInstance DEFAULT = new MuxFileDirectoryCacheInstance.Builder().build();

    public static boolean tryEvict(MuxFileDirectory muxDir) {
        return DEFAULT.tryEvict(muxDir);
    }

    public static boolean tryClear() {
        return DEFAULT.tryClear();
    }

    public static int getCacheDirSize() {
        return DEFAULT.getCacheDirSize();
    }

    public static int getAndClearCacheEvictions() {
        return DEFAULT.getAndClearCacheEvictions();
    }

    public static long getCacheByteSize() {
        return DEFAULT.getCacheByteSize();
    }

    public static long getCacheStreamSize() {
        return DEFAULT.getCacheStreamSize();
    }

    public static int getCacheFileSize() {
        return DEFAULT.getCacheFileSize();
    }

    /* returns an authoritative instance of an MFM for a given directory */
    private static TrackedMultiplexFileManager getAuthoritativeInstance(File dir) throws Exception {
        return DEFAULT.getAuthoritativeInstance(dir.toPath());
    }

    /* returns an authoritative instance of an MFM for a given directory */
    private static TrackedMultiplexFileManager getAuthoritativeInstance(Path dir) throws Exception {
        return DEFAULT.getAuthoritativeInstance(dir);
    }

    public static void waitForWriteClosure() {
        DEFAULT.waitForWriteClosure();
    }

    /* open and cache for min of 60 seconds */
    public static TrackedMultiplexFileManager getWriteableInstance(File dir) throws Exception {
        return DEFAULT.getWriteableInstance(dir);
    }
}
