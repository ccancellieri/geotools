package org.geotools.data.cache.impl;

import java.io.IOException;

import org.geotools.data.Query;
import org.geotools.data.cache.op.BaseCachedFeatureSource;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.cache.op.FeatureSourceOp;
import org.geotools.data.simple.SimpleFeatureSource;

public class STRFeatureSourceOp extends FeatureSourceOp {

    public STRFeatureSourceOp(CacheManager cacheManager, final String uid) throws IOException {
        super(cacheManager, uid);
    }
    
    @Override
    public BaseCachedFeatureSource getCache(Query query) throws IOException {
        verify(query);
        BaseCachedFeatureSource s = ehCacheGet(ehCacheManager, this.getUid());
        if (s == null) {
            s=updateCache(query);
            setCached(s!=null?true:false, this.getUid());
        } else {
            if (!isCached(this.getUid())){
                s=updateCache(query);
            }
        }
        return s;
    }

    @Override
    public BaseCachedFeatureSource updateCache(Query query) throws IOException {
        final BaseCachedFeatureSource str=new STRCachedFeatureSource(cacheManager, getEntry(), query);
        ehCachePut(ehCacheManager, str, this.getUid());
        return str;
    }
}
