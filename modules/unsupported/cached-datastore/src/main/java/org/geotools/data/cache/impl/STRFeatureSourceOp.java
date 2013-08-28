package org.geotools.data.cache.impl;

import java.io.IOException;

import org.geotools.data.Query;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.cache.op.FeatureSourceOp;
import org.geotools.data.simple.SimpleFeatureSource;

public class STRFeatureSourceOp extends FeatureSourceOp {

    public STRFeatureSourceOp(CacheManager cacheManager, final String uid) throws IOException {
        super(cacheManager, uid);
    }
    
    @Override
    public SimpleFeatureSource getCache(Query query) throws IOException {
        verify(query);
        final SimpleFeatureSource s = ehCacheGet(ehCacheManager, this.getUid());
        if (s == null) {
            setCached(updateCache(query)!=null?true:false, query);
        }
        return s;
    }

    @Override
    public SimpleFeatureSource updateCache(Query query) throws IOException {
        final SimpleFeatureSource str=new STRCachedFeatureSource(cacheManager, getEntry(), null);
        ehCachePut(ehCacheManager, str, query);
        return str;
    }
}
