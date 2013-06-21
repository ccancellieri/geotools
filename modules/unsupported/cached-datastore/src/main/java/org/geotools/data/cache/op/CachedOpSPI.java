package org.geotools.data.cache.op;

import java.io.IOException;
import java.io.Serializable;

import org.geotools.data.DataStore;

public abstract class CachedOpSPI<T extends CachedOp> implements Serializable {

    /** serialVersionUID */
    private static final long serialVersionUID = -7808528781956318808L;

    public T create(DataStore source, DataStore cache, CacheManager cacheManager,
            final String uniqueName) throws IOException {
        return createInstance(cacheManager, uniqueName);
    }

    protected abstract T createInstance(CacheManager cacheManager, final String uniqueName)
            throws IOException;

    /**
     * @return The cached operation (an {@link Operation})
     */
    public abstract Operation getOp();

    /**
     * @return The cached operation priority
     */
    public abstract long priority();

    /**
     * Overriding the hash code so we can use CachedOpSPI or Operation transparently in maps
     */
    @Override
    public int hashCode() {
        return getOp().hashCode();
    }
}
