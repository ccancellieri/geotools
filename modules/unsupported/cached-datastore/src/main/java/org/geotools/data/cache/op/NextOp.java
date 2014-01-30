package org.geotools.data.cache.op;

import java.io.IOException;
import java.util.logging.Logger;

import org.opengis.feature.simple.SimpleFeature;

public class NextOp extends BaseOp<SimpleFeature, SimpleFeature> {

    protected final transient Logger LOGGER = org.geotools.util.logging.Logging
            .getLogger(getClass().getPackage().getName());

    public NextOp(CacheManager cacheManager, final String uniqueName) throws IOException {
        super(cacheManager, uniqueName);
    }

    @Override
    public SimpleFeature updateCache(SimpleFeature sf) throws IOException {
        verify(sf);

        return sf;

    }

    @Override
    public SimpleFeature getCache(SimpleFeature o) throws IOException {
        return updateCache(o);
    }

    @Override
    public void setCached(SimpleFeature key, boolean isCached) {
        // do nothing
    };

    @Override
    public boolean isCached(SimpleFeature o) throws IOException {
        return true;
    }

    @Override
    public boolean isDirty(SimpleFeature key) throws IOException {
        return false;
    }

    @Override
    public void setDirty(SimpleFeature query, boolean value) throws IOException {
        throw new UnsupportedOperationException();
    }

}
