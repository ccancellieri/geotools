package org.geotools.data.cache.impl;

import java.io.IOException;

import org.geotools.data.DataStore;
import org.geotools.data.cache.op.BaseFeatureSourceOp;
import org.geotools.data.cache.utils.CacheUtils;
import org.geotools.data.cache.utils.CachedFeatureSource;
import org.geotools.data.simple.SimpleFeatureSource;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

public class STRFeatureSourceOp extends BaseFeatureSourceOp {

    Cache cacheManager;

    public STRFeatureSourceOp(DataStore ds, String typeName) {
        super(ds, null, typeName);
        cacheManager = CacheUtils.getCacheUtils().getCache("featureSource");
    }

    @Override
    public SimpleFeatureSource getCached() throws IOException {
        SimpleFeatureSource s=cacheGet(cacheManager, this.hashCode());
        if (s == null) {
            SimpleFeatureSource fs = store.getFeatureSource(typeName);
            if (fs != null) {
                s = new STRCachedFeatureSource(fs, getEntry(), null);
                cachePut(cacheManager, s, this.hashCode());
            } else {
                throw new IOException(
                        "Unable to cache a null feature source, please check the source datastore.");
            }
        }
        return s;
    }

    public static <T> void cachePut(Cache cacheManager, T value, Object... keys) throws IOException {
        if (value != null) {
            cacheManager.put(keys, value);
        } else {
            throw new IOException(
                    "Unable to cache a null Object, please check the source datastore.");
        }
    }

    public static <T> T cacheGet(Cache cacheManager, Object... keys) {
        SimpleValueWrapper vw = (SimpleValueWrapper) cacheManager.get(keys);
        if (vw != null) {
            return (T) vw.get();
        } else {
            return null;
        }
    }

    @Override
    public boolean cache(SimpleFeatureSource arg) throws IOException {
        return true; // unused: skipped
    }

    @Override
    protected SimpleFeatureSource getCachedInternal() throws IOException {
        return null; // unused: skipped
    }

}
