package org.geotools.data.cache.op.feature.reader;

import java.io.IOException;

import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.feature.BaseFeatureOp;
import org.geotools.data.cache.op.feature.BaseFeatureOpStatus;
import org.geotools.data.cache.utils.PipelinedContentFeatureReader;
import org.geotools.data.simple.SimpleFeatureReader;

public class FeatureReaderOp extends BaseFeatureOp<SimpleFeatureReader> {

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
        return new PipelinedContentFeatureReader(getStatus(), query, cacheManager,
                Transaction.AUTO_COMMIT);
//        return new org.geotools.data.cache.utils.SimpleFeatureReader(getStatus().getEntry(), query, cacheManager);
    }
}
