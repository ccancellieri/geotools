package org.geotools.data.cache.op.feature.bounds;

import java.io.IOException;

import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.cache.op.feature.BaseFeatureOpSPI;
import org.geotools.data.cache.op.feature.BaseFeatureOpStatus;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.springframework.stereotype.Service;

@Service
public class FeatureBoundsOpSPI extends BaseFeatureOpSPI<FeatureBoundsOp, ReferencedEnvelope> {

    /** serialVersionUID */
    private static final long serialVersionUID = 8822095832437682003L;

    @Override
    public Operation getOp() {
        return Operation.featureBounds;
    }

    @Override
    public FeatureBoundsOp createInstance(CacheManager cacheManager,
            final BaseFeatureOpStatus status) throws IOException {
        return new FeatureBoundsOp(cacheManager, status);
    }

}
