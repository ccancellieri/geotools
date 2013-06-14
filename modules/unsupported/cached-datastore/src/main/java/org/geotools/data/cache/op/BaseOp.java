package org.geotools.data.cache.op;

import java.io.IOException;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.simple.SimpleFeatureSource;

public abstract class BaseOp<T> implements CachedOp<T> {

    // cached datastore
    protected final DataStore cache;

    // cached datastore
    protected final DataStore store;
    
    protected final CacheManager cacheManager;

    protected boolean isCached = false;

    public BaseOp(DataStore ds, DataStore cds, CacheManager cacheManager) {
        if (ds == null)
            throw new IllegalArgumentException("Unable to initialize the store with a null store");
        this.cache = cds;
        this.store = ds;
        this.cacheManager=cacheManager;
    }

    @Override
    public T getCached() throws IOException {
        T op = null;
        if (!isCached) {
            op = operation();
            isCached = cache(op);
        }
        if (isCached) {
            return getCachedInternal();
        } else {
            return op;
        }
    }

    protected abstract T getCachedInternal() throws IOException;

}
