package org.geotools.data.cache.op;

import java.io.IOException;
import java.io.Serializable;

public abstract class CachedOpSPI<T extends BaseOp<?, ?>> implements Serializable {

    /** serialVersionUID */
    private static final long serialVersionUID = -7808528781956318808L;

    public T create(CacheManager cacheManager, final String uid) throws IOException {
        return createInstance(cacheManager, uid);
    }

    @Override
    public boolean equals(Object o) {
        if (this.getClass().isAssignableFrom(o.getClass())) {
            CachedOpSPI<T> op = (CachedOpSPI<T>) o;
            if (op.getClass().equals(this.getClass()) && this.getOp().equals(op.getOp())
                    && this.priority() == op.priority()) {
                return true;
            }
        }
        return false;
    }

    protected abstract T createInstance(CacheManager cacheManager, final String uid)
            throws IOException;

    /**
     * @return The cached operation (an {@link Operation})
     */
    public abstract Operation getOp();

    /**
     * @return The cached operation priority
     */
    public abstract long priority();

    // /**
    // * Overriding the hash code so we can use CachedOpSPI or Operation transparently in maps
    // */
    // @Override
    // public int hashCode() {
    // return getOp().hashCode();
    // }
    //
    // /**
    // * Overriding equals so we can search CachedOpSPI using Operation transparently in maps
    // */
    // @Override
    // public boolean equals(Object obj) {
    // return getOp().equals(obj);
    // }
}
