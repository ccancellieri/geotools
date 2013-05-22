package org.geotools.data.cache.op;

import java.io.IOException;

import org.geotools.data.DataStore;

public abstract class CachedOpSPI<T extends CachedOp<?>> {

    public T create(DataStore source, DataStore cache) throws IOException {
        return createInstance(source,cache);
    }
    
    protected abstract T createInstance(DataStore source, DataStore cache) throws IOException;

    /**
     * @return The cached operation (usually a {@link CacheOp})
     */
    public abstract Object getOp();
}
