package org.geotools.data.cache.op.feature.reader;

import java.io.IOException;

import org.geotools.data.Query;
import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.BaseOp;
import org.geotools.data.cache.op.feature.BaseFeatureOpStatus;
import org.geotools.data.simple.SimpleFeatureReader;

public class FeatureReaderOp extends BaseOp<BaseFeatureOpStatus,Query,SimpleFeatureReader> {

    public FeatureReaderOp(final CacheManager cacheManager, final BaseFeatureOpStatus status)
            throws IOException {
        super(cacheManager, status);
    }

    @Override
    public SimpleFeatureReader getCache(Query query) throws IOException {
        return new org.geotools.data.cache.utils.SimpleFeatureReader(getStatus().getEntry(), query, cacheManager);
    }

    @Override
    public SimpleFeatureReader updateCache(Query query) throws IOException {
        return new org.geotools.data.cache.utils.SimpleFeatureReader(getStatus().getEntry(), query, cacheManager);
    }
}
