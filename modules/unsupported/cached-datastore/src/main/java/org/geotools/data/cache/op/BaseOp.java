package org.geotools.data.cache.op;

import java.io.IOException;

import org.geotools.data.DataStore;
import org.geotools.data.simple.SimpleFeatureSource;

public abstract class BaseOp<T> implements CachedOp<T> {

    // cached datastore
    protected final DataStore cache;

    // cached datastore
    protected final DataStore store;

    protected boolean isCached = false;

    public BaseOp(DataStore ds, DataStore cds) {
        if (ds == null)
            throw new IllegalArgumentException("Unable to initialize the store with a null store");
        cache = cds;
        store = ds;
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
