package org.geotools.data.cache.impl;

import java.io.IOException;

import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.cache.op.CachedOpSPI;
import org.geotools.data.cache.op.Operation;
import org.springframework.stereotype.Component;

@Component
public class STRFeatureCollectionOpSPI extends CachedOpSPI<STRFeatureCollectionOp> {

    @Override
    public Operation getOp() {
        return Operation.featureCollection;
    }

    @Override
    protected STRFeatureCollectionOp createInstance(CacheManager cacheManager, final String uniqueName) throws IOException {
        return new STRFeatureCollectionOp(cacheManager,uniqueName);
    }

    @Override
    public long priority() {
        return 0;
    }
}
