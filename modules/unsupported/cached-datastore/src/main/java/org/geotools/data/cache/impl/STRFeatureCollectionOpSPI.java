package org.geotools.data.cache.impl;

import java.io.IOException;

import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.cache.op.CachedOpSPI;
import org.geotools.data.cache.op.Operation;
import org.springframework.stereotype.Service;

@Service
public class STRFeatureCollectionOpSPI extends CachedOpSPI<STRFeatureCollectionOp> {

    /** serialVersionUID */
    private static final long serialVersionUID = -3369795418730108696L;

    @Override
    public Operation getOp() {
        return Operation.featureCollection;
    }

    @Override
    protected STRFeatureCollectionOp createInstance(CacheManager cacheManager, final String uid) throws IOException {
        return new STRFeatureCollectionOp(cacheManager,uid);
    }

    @Override
    public long priority() {
        return 0;
    }
}
