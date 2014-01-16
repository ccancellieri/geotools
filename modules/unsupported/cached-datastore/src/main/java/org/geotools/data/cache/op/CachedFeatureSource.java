package org.geotools.data.cache.op;

import java.io.IOException;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.store.ContentEntry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

@SuppressWarnings("unchecked")
public class CachedFeatureSource extends BaseCachedFeatureSource {

    public CachedFeatureSource(CacheManager cacheManager, ContentEntry entry, Query query)
            throws IllegalArgumentException, IOException {
        super(cacheManager, entry, query);
    }

    @Override
    protected int count(Query origQuery, Query integratedQuery) throws IOException {
        return this.cacheManager.getCache().getFeatureSource(origQuery.getTypeName()).getCount(origQuery);
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> query(Query origQuery, Query integratedQuery) throws IOException {
        return this.cacheManager.getCache().getFeatureReader(origQuery, Transaction.AUTO_COMMIT);
    }

}
