package org.geotools.data.cache.op;

import java.io.IOException;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.cache.utils.DelegateContentFeatureSource;
import org.geotools.data.cache.utils.PipelinedContentFeatureReader;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class CountOp extends BaseFeatureSourceOp<Integer> {

    public CountOp(final CacheManager cacheManager, final String uid) throws IOException {
        super(cacheManager, uid);
    }

    @Override
    public Integer getCache(Query query) throws IOException {
        return cacheManager.getCache().getFeatureSource(query.getTypeName()).getCount(query);
    }

    @Override
    public Integer updateCache(Query query) throws IOException {
        final SimpleFeatureSource featureSource = new DelegateContentFeatureSource(cacheManager,
                getEntry(), query) {
            @Override
            protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
                    throws IOException {

                if (isCached(query) && !isDirty(query)) {
                    return cacheManager.getCache().getFeatureReader(query, transaction);
                } else {
                    return new PipelinedContentFeatureReader(getEntry(), query,
                            integrateCachedQuery(query), cacheManager, transaction);
                }
            }
        };
        return featureSource.getCount(query);
    }

}
