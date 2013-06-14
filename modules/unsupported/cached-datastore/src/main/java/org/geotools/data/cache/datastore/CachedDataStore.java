package org.geotools.data.cache.datastore;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.cache.op.BaseFeatureSourceOp;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.cache.op.CachedOp;
import org.geotools.data.cache.utils.CacheUtils;
import org.geotools.data.cache.utils.DelegateContentFeatureSource;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.opengis.feature.type.Name;

public class CachedDataStore extends ContentDataStore {

    // cached datastore
    private final DataStore store;

    private final DataStore cache;

    private final CacheManager cacheManager;

    // public CachedDataStore(DataStore store, DataStore cache) throws IOException {
    // this(store, cache, new CacheManager(store, cache));
    // }

    public CachedDataStore(DataStore store, final CacheManager cacheMap) throws IOException {
        this(store, null, cacheMap);
    }

    public CachedDataStore(DataStore store, DataStore cache, final CacheManager cacheManager)
            throws IOException {
        if (store == null)
            throw new IllegalArgumentException("Unable to initialize the store with a null store");

        this.store = store;
        this.cache = cache;
        this.cacheManager = cacheManager;

    }

    @Override
    protected List<Name> createTypeNames() throws IOException {
        CachedOp<?> cachedOp = cacheManager.getCachedOp(Operation.typeNames);
        if (cachedOp != null)
            return (List<Name>) cachedOp.getCached();
        else
            return store.getNames();
    }

    public DataStore getStore() {
        return store;
    }

    @Override
    protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
        BaseFeatureSourceOp featureSourceOp = (BaseFeatureSourceOp) cacheManager
                .getCachedOp(Operation.featureSource);
        if (featureSourceOp != null) {
            featureSourceOp.setTypeName(entry.getTypeName());
            featureSourceOp.setEntry(entry);
            return new DelegateContentFeatureSource(entry, null, featureSourceOp.getCached());
        } else
            return new DelegateContentFeatureSource(entry, null, store.getFeatureSource(entry
                    .getTypeName()));
    }
}
