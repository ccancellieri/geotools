package org.geotools.data.cache.op.feature.source;

import java.io.IOException;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.CachedOpStatus;
import org.geotools.data.cache.op.feature.BaseFeatureOp;
import org.geotools.data.cache.op.feature.BaseFeatureOpStatus;
import org.geotools.data.cache.utils.DelegateContentFeatureSource;
import org.geotools.data.cache.utils.PipelinedContentFeatureReader;
import org.geotools.data.cache.utils.SimpleFeatureUpdaterReader;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class FeatureSourceOp extends BaseFeatureOp<SimpleFeatureSource> {

    public FeatureSourceOp(final CacheManager cacheManager, final CachedOpStatus<Query> status) throws IOException {
        super(cacheManager, status);
    }

    @Override
    public SimpleFeatureSource getCache(Query query) throws IOException {
        final SimpleFeatureSource featureSource = new DelegateContentFeatureSource(cacheManager,
                query, (BaseFeatureOpStatus)getStatus()) {
            @Override
            protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
                    throws IOException {
                return new SimpleFeatureUpdaterReader(getEntry(), query, cacheManager,
                        Transaction.AUTO_COMMIT);
            }
        };
        return featureSource;
    }

    @Override
    public SimpleFeatureSource updateCache(Query query) throws IOException {

        final SimpleFeatureSource featureSource = new DelegateContentFeatureSource(cacheManager,
                query, (BaseFeatureOpStatus)getStatus()) {
            @Override
            protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
                    throws IOException {

                return new PipelinedContentFeatureReader((BaseFeatureOpStatus)getStatus(), query, cacheManager,
                        Transaction.AUTO_COMMIT);
            }
        };
        return featureSource;
    }
}
