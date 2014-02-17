package org.geotools.data.cache.op;

import java.io.IOException;

import org.geotools.data.cache.datastore.CacheManager;

public abstract class BaseOp<S extends CachedOpStatus<K>, K, T> implements CachedOp<K, T> {

    // manager
    protected final transient CacheManager cacheManager;

    protected final S status;

    public BaseOp(CacheManager cacheManager, final S status) throws IOException {

        if (status == null || cacheManager == null)
            throw new IllegalArgumentException(
                    "Unable to build a CachedOp without the cacheManager or the status");

        this.cacheManager = cacheManager;

        this.status = status;
    }

    public S getStatus() {
        return status;
    }

    public void clear() throws IOException {
        status.clear();
    }

    public boolean isDirty(K key) throws IOException {
        return status.isDirty(key);
    }

    public void setDirty(K key, boolean value) throws IOException {
        status.setDirty(key, value);
    }

    public boolean isCached(K key) throws IOException {
        return status.isCached(key);
    }

    public void setCached(K key, boolean isCached) throws IOException {
        status.setCached(key, isCached);
    }

    @Override
    public void dispose() throws IOException {
    }

}
