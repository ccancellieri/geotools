package org.geotools.data.cache.op.next;

import java.io.IOException;
import java.util.logging.Logger;

import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.BaseOp;
import org.geotools.data.cache.op.CachedOpStatus;
import org.opengis.feature.simple.SimpleFeature;

public class NextOp extends BaseOp<SimpleFeature, SimpleFeature> {

    protected final transient Logger LOGGER = org.geotools.util.logging.Logging
            .getLogger(getClass().getPackage().getName());

    public NextOp(CacheManager cacheManager, final CachedOpStatus<SimpleFeature> status)
            throws IOException {
        super(cacheManager, status);
    }

    @Override
    public SimpleFeature updateCache(SimpleFeature sf) throws IOException {
        return sf;
    }

    @Override
    public SimpleFeature getCache(SimpleFeature o) throws IOException {
        return updateCache(o);
    }

}
