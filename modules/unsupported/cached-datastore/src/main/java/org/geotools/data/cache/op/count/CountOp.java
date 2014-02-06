package org.geotools.data.cache.op.count;

import java.io.IOException;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.CachedOpStatus;
import org.geotools.data.cache.op.feature.BaseFeatureOp;
import org.geotools.data.cache.op.feature.BaseFeatureOpStatus;
import org.geotools.data.cache.utils.DelegateContentFeatureSource;
import org.geotools.data.cache.utils.PipelinedContentFeatureReader;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class CountOp extends BaseFeatureOp<Integer> {

    public CountOp(final CacheManager cacheManager, final CachedOpStatus<Query> status) throws IOException {
        super(cacheManager, status);
    }

    @Override
    public Integer getCache(Query query) throws IOException {
        return cacheManager.getCache().getFeatureSource(query.getTypeName()).getCount(query);
    }

    @Override
    public Integer updateCache(Query query) throws IOException {

        final SimpleFeatureSource featureSource = new DelegateContentFeatureSource(cacheManager, query, (BaseFeatureOpStatus)getStatus()) {
            @Override
            protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
                    throws IOException {

                if (isCached(query) && !isDirty(query)) {
                    return cacheManager.getCache().getFeatureReader(query, transaction);
                } else {
                    return new PipelinedContentFeatureReader((BaseFeatureOpStatus) getStatus(), query, 
                            cacheManager, transaction);
                }
            }
        };
        return featureSource.getCount(query);
    }

}
