package org.geotools.data.cache.op.feature.reader;

import java.io.IOException;

import org.geotools.data.Query;
import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.BaseOpSPI;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.cache.op.feature.BaseFeatureOpStatus;
import org.geotools.data.simple.SimpleFeatureReader;
import org.springframework.stereotype.Service;

@Service
public class FeatureReaderOpSPI extends
        BaseOpSPI<BaseFeatureOpStatus, FeatureReaderOp, Query, SimpleFeatureReader> {

    /** serialVersionUID */
    private static final long serialVersionUID = 8822095832437682003L;

    @Override
    public Operation getOp() {
        return Operation.featureReader;
    }

    @Override
    public FeatureReaderOp createInstance(CacheManager cacheManager,
            final BaseFeatureOpStatus status) throws IOException {
        return new FeatureReaderOp(cacheManager, status);
    }

    @Override
    public BaseFeatureOpStatus createStatus() {
        return new BaseFeatureOpStatus();
    }
}
