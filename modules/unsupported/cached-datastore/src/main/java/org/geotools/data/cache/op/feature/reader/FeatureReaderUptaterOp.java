package org.geotools.data.cache.op.feature.reader;

import java.io.IOException;

import org.geotools.data.Query;
import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.CachedOpStatus;
import org.geotools.data.cache.op.feature.BaseFeatureOp;
import org.geotools.data.cache.op.feature.BaseFeatureOpStatus;
import org.geotools.data.cache.utils.SimpleFeatureUpdaterReader;
import org.geotools.data.simple.SimpleFeatureReader;

public class FeatureReaderUptaterOp extends BaseFeatureOp<SimpleFeatureReader> {

    public FeatureReaderUptaterOp(final CacheManager cacheManager, final CachedOpStatus<Query> status)
            throws IOException {
        super(cacheManager, status);
    }

    @Override
    public SimpleFeatureReader getCache(Query query) throws IOException {
        return new SimpleFeatureUpdaterReader(((BaseFeatureOpStatus)getStatus()).getEntry(), query, cacheManager);
    }

    @Override
    public SimpleFeatureReader updateCache(Query query) throws IOException {

        return new SimpleFeatureUpdaterReader(((BaseFeatureOpStatus)getStatus()).getEntry(), query, cacheManager);
    }
}
