package org.geotools.data.cache.op.feature.count;

import java.io.IOException;

import org.geotools.data.Query;
import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.BaseOpSPI;
import org.geotools.data.cache.op.BaseOpStatus;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.cache.op.feature.BaseFeatureOpStatus;
import org.springframework.stereotype.Service;

@Service
public class CountOpSPI extends BaseOpSPI<BaseFeatureOpStatus, CountOp, Query, Integer> {

    /** serialVersionUID */
    private static final long serialVersionUID = 8822095832437602003L;

    @Override
    public Operation getOp() {
        return Operation.featureCount;
    }

    @Override
    public CountOp createInstance(CacheManager cacheManager, final BaseFeatureOpStatus status)
            throws IOException {
        return new CountOp(cacheManager, status);
    }

    @Override
    public BaseFeatureOpStatus createStatus() {
        return new BaseFeatureOpStatus() {

            @Override
            public boolean isApplicable(Operation op) {
                return op.equals(Operation.featureCount) ? true : false;
            }

        };
    }
}
