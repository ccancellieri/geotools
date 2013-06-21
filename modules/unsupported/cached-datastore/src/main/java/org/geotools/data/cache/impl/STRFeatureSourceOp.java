package org.geotools.data.cache.impl;

import java.io.IOException;

import org.geotools.data.cache.op.BaseFeatureSourceOp;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.cache.utils.EHCacheUtils;
import org.geotools.data.simple.SimpleFeatureSource;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

public class STRFeatureSourceOp extends BaseFeatureSourceOp {

    final Cache ehCacheManager;

    public STRFeatureSourceOp(CacheManager cacheManager, final String uniqueName) {
        super(cacheManager,uniqueName);
        this.ehCacheManager = EHCacheUtils.getCacheUtils().getCache("featureSource");
        
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
//    @Override
//    public transient boolean isCached(String[] o) {
//        
//    };
//    
//    @Override
//    public transient void setCached(boolean isCached, String[] key) {
//        
//    };

    @Override
    public SimpleFeatureSource getCache(String... typeName) throws IOException {
        SimpleFeatureSource s = cacheGet(ehCacheManager, this.hashCode());
        if (s == null) {
            SimpleFeatureSource fs = source.getFeatureSource(typeName[0]);
            if (fs != null) {
                s = new STRCachedFeatureSource(cacheManager,fs, getEntry(), null);
                cachePut(ehCacheManager, s, this.hashCode());
            } else {
                throw new IOException(
                        "Unable to cache a null feature source, please check the source datastore.");
            }
        }
        return s;
    }

    @Override
    public boolean putCache(SimpleFeatureSource... source) throws IOException {
        // TODO Auto-generated method stub
        cachePut(ehCacheManager, new STRCachedFeatureSource(cacheManager,source[0], getEntry(), null), this.hashCode());
        return true;
    }


}
