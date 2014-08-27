package com.addthis.muxy;

import java.nio.file.Path;

import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.util.concurrent.Futures.immediateFuture;

/** Resolves symlinks and creates new objects only when the symlink has changed. */
class ReadMuxFileDirectoryCacheLoader extends CacheLoader<Path, ReadMuxFileDirectory> {
    @Override
    public ReadMuxFileDirectory load(final Path key) throws Exception {
        return new ReadMuxFileDirectory(key.toRealPath());
    }

    @Override
    public ListenableFuture<ReadMuxFileDirectory> reload(final Path key,
                                                         final ReadMuxFileDirectory oldValue) throws Exception {
        if (key.toRealPath().equals(oldValue.getDirectory())) {
            return immediateFuture(oldValue);
        } else {
            return immediateFuture(load(key));
        }
    }
}
