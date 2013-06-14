package org.geotools.data.cache.op;

import java.io.IOException;

import org.geotools.data.DataStore;

public abstract class CachedOpSPI<T extends CachedOp<?>> {

    public T create(DataStore source, DataStore cache, CacheManager cacheManager)
            throws IOException {
        return createInstance(source, cache, cacheManager);
    }

    protected abstract T createInstance(DataStore source, DataStore cache, CacheManager cacheManager)
            throws IOException;

    /**
     * @return The cached operation (an {@link Operation})
     */
    public abstract Operation getOp();

    /**
     * @return The cached operation priority
     */
    public abstract long priority();
}
