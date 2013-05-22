package org.geotools.data.cache.datastore;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.cache.op.BaseFeatureSourceOp;
import org.geotools.data.cache.op.CacheOp;
import org.geotools.data.cache.op.CachedOp;
import org.geotools.data.cache.op.CachedOpSPI;
import org.geotools.data.cache.utils.CacheUtils;
import org.geotools.data.cache.utils.DelegateContentFeatureSource;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.opengis.feature.type.Name;

public class CachedDataStore extends ContentDataStore {

    // cached datastore
    private final DataStore store;

    private final DataStore cache;

    private Map<Object, CachedOp<?>> cacheMap;

    private Map<Object, CachedOpSPI<?>> cacheConfig;

    public CachedDataStore(DataStore store, DataStore cache) throws IOException {
        this(store, cache, null);
    }

    public CachedDataStore(DataStore store, DataStore cache, final Map<Object, CachedOp<?>> cacheMap)
            throws IOException {
        if (store == null)
            throw new IllegalArgumentException("Unable to initialize the store with a null store");

        this.store = store;
        this.cache = cache;
        // need lazy initialization: we have to wait for the spring context loading
        this.cacheMap = cacheMap != null ? cacheMap : null;
    }

    public Map<Object, CachedOp<?>> getCacheMap() throws IOException {

        if (this.cacheMap == null) {
            final CacheUtils cu = CacheUtils.getCacheUtils();
            if (cu != null) {
                cacheMap = cu.buildCache(store, cache);
            } else {
                return new HashMap<Object, CachedOp<?>>();
            }
        }
        return cacheMap;
    }

    @Override
    protected List<Name> createTypeNames() throws IOException {
        CachedOp<?> cachedOp = getCacheMap().get(CacheOp.typeNames);
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
        BaseFeatureSourceOp featureSourceOp = (BaseFeatureSourceOp) getCacheMap().get(
                CacheOp.featureSource);
        if (featureSourceOp != null) {
            featureSourceOp.setTypeName(entry.getTypeName());
            featureSourceOp.setEntry(entry);
            return new DelegateContentFeatureSource(entry, null, featureSourceOp.getCached());
        } else
            return new DelegateContentFeatureSource(entry, null, store.getFeatureSource(entry
                    .getTypeName()));
    }
}
