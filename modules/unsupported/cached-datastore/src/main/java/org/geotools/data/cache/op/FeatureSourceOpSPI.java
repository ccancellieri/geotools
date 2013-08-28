package org.geotools.data.cache.op;

import java.io.IOException;

import org.springframework.stereotype.Service;

@Service
public class FeatureSourceOpSPI extends CachedOpSPI<FeatureSourceOp> {

    /** serialVersionUID */
    private static final long serialVersionUID = 8822095832437682003L;

    @Override
    public Operation getOp() {
        return Operation.featureSource;
    }

    @Override
    protected FeatureSourceOp createInstance(CacheManager cacheManager, final String uniqueName)
            throws IOException {
        return new FeatureSourceOp(cacheManager, uniqueName);
    }

    @Override
    public long priority() {
        return 0;
    }
}
