package org.geotools.data.cache.impl;

import java.io.IOException;

import org.geotools.data.DataStore;
import org.geotools.data.cache.op.CacheOp;
import org.geotools.data.cache.op.CachedOpSPI;
import org.springframework.stereotype.Component;

@Component
public class STRFeatureSourceOpSPI extends CachedOpSPI<STRFeatureSourceOp> {

    @Override
    protected STRFeatureSourceOp createInstance(DataStore source, DataStore cache)
            throws IOException {
        return new STRFeatureSourceOp(source, null);
    }

    @Override
    public Object getOp() {
        return CacheOp.featureSource;
    }
}
