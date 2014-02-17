package org.geotools.data.cache.op;

import java.io.IOException;
import java.io.Serializable;

import org.geotools.data.cache.datastore.CacheManager;

public abstract class CachedOpSPI<S extends CachedOpStatus<K>, E extends CachedOp<K,T>, K, T> implements Serializable {

    /** serialVersionUID */
    private static final long serialVersionUID = -7808528781956318808L;

    public E create(CacheManager cacheManager, final S status) throws IOException {
        return createInstance(cacheManager, status);
    }

    @Override
    public boolean equals(Object o) {
        if (this.getClass().isAssignableFrom(o.getClass())) {
            CachedOpSPI<S,E,K,T> op = (CachedOpSPI<S,E,K,T>) o;
            if (op.getClass().equals(this.getClass()) && this.getOp().equals(op.getOp())
                    && this.priority() == op.priority()) {
                return true;
            }
        }
        return false;
    }

    public abstract E createInstance(CacheManager cacheManager, final S status)
            throws IOException;
    
    public abstract S createStatus();

    /**
     * @return The cached operation (an {@link Operation})
     */
    public abstract Operation getOp();

    /**
     * @return The cached operation priority
     */
    public abstract long priority();

}
