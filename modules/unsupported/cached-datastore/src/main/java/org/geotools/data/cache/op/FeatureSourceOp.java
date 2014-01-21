package org.geotools.data.cache.op;

import java.io.IOException;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.cache.utils.DelegateContentFeatureSource;
import org.geotools.data.cache.utils.PipelinedContentFeatureReader;
import org.geotools.data.cache.utils.SimpleFeatureUpdaterReader;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class FeatureSourceOp extends BaseFeatureSourceOp<SimpleFeatureSource> {

    public FeatureSourceOp(final CacheManager cacheManager, final String uid) throws IOException {
        super(cacheManager, uid);
    }

    @Override
    public SimpleFeatureSource getCache(Query query) throws IOException {
        final SimpleFeatureSource featureSource = new DelegateContentFeatureSource(cacheManager,
                getEntry(), query) {
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
        // pre-calculate query before we call setCache(true) (which will modify areas!!!)
        final Query cacheQuery = queryCachedAreas(query);
        final Query sourceQuery = querySource(query);

        final SimpleFeatureSource featureSource = new DelegateContentFeatureSource(cacheManager,
                getEntry(), query) {
            @Override
            protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
                    throws IOException {

                return new PipelinedContentFeatureReader(getEntry(), sourceQuery, cacheQuery,
                        cacheManager, Transaction.AUTO_COMMIT);
            }
        };
        return featureSource;
    }

}
