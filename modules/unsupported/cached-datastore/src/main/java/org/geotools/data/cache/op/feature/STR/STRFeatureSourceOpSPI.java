package org.geotools.data.cache.op.feature.STR;

import java.io.IOException;

import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.CachedOpSPI;
import org.geotools.data.cache.op.Operation;
import org.springframework.stereotype.Service;

@Service
public class STRFeatureSourceOpSPI extends BaseOpSPI<STRFeatureSourceOp> {

    /** serialVersionUID */
    private static final long serialVersionUID = 8822095832437682003L;

    @Override
    public Operation getOp() {
        return Operation.featureSource;
    }

    @Override
    protected STRFeatureSourceOp createInstance(CacheManager cacheManager, final String uniqueName) throws IOException {
        return new STRFeatureSourceOp(cacheManager,uniqueName);
    }

    @Override
    public long priority() {
        return 1;
    }
}
