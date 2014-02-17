package org.geotools.data.cache.op.feature;

import org.geotools.data.Query;
import org.geotools.data.cache.op.BaseOpSPI;
import org.geotools.data.cache.op.BaseOpStatus;
import org.geotools.data.cache.op.CachedOp;
import org.geotools.data.cache.op.CachedOpStatus;
import org.springframework.stereotype.Service;

@Service
public abstract class BaseFeatureOpSPI<E extends CachedOp<Query, T>, T> extends BaseOpSPI<BaseFeatureOpStatus, E, Query, T> {

    @Override
    public BaseFeatureOpStatus createStatus() {
        return new BaseFeatureOpStatus();
    }
}
