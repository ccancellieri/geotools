package org.geotools.data.cache.op.feature.source;

import java.io.IOException;

import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.cache.op.feature.BaseFeatureOpSPI;
import org.geotools.data.cache.op.feature.BaseFeatureOpStatus;
import org.geotools.data.simple.SimpleFeatureSource;
import org.springframework.stereotype.Service;

@Service
public class FeatureSourceOpSPI extends BaseFeatureOpSPI<FeatureSourceOp, SimpleFeatureSource> {

    /** serialVersionUID */
    private static final long serialVersionUID = 8822095832437682003L;

    @Override
    public Operation getOp() {
        return Operation.featureSource;
    }

    @Override
    public FeatureSourceOp createInstance(CacheManager cacheManager,
            final BaseFeatureOpStatus status) throws IOException {
        return new FeatureSourceOp(cacheManager, status);
    }

}
