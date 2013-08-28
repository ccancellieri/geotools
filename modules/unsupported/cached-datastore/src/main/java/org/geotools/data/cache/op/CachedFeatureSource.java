package org.geotools.data.cache.op;

import java.io.IOException;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.store.ContentEntry;

@SuppressWarnings("unchecked")
public class CachedFeatureSource extends BaseCachedFeatureSource {

    public CachedFeatureSource(CacheManager cacheManager, ContentEntry entry, Query query)
            throws IllegalArgumentException, IOException {
        super(cacheManager, entry, query);
    }

    @Override
    protected int count(Query query) throws IOException {
        return this.cacheManager.getCache().getFeatureSource(query.getTypeName()).getCount(query);
    }

    @Override
    protected FeatureReader<?, ?> query(Query query) throws IOException {
        return this.cacheManager.getCache().getFeatureReader(query, Transaction.AUTO_COMMIT);
    }

}
