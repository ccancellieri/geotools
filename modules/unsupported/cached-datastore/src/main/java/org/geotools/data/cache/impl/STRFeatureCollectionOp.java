package org.geotools.data.cache.impl;

import java.io.IOException;

import org.geotools.data.Query;
import org.geotools.data.cache.op.BaseOp;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.type.Name;

public class STRFeatureCollectionOp extends BaseOp<SimpleFeatureCollection, Query> {

    private final STRFeatureSourceOp strOp;

    public STRFeatureCollectionOp(CacheManager cacheManager, final String uid) throws IOException {
        super(cacheManager, uid);
        strOp = new STRFeatureSourceOp(cacheManager, getUid());
    }

    @Override
    public SimpleFeatureCollection getCache(Query query) throws IOException {
        verify(query);
        SimpleFeatureSource s = strOp.getCache(query);
        return s.getFeatures();
    }

    @Override
    public SimpleFeatureCollection updateCache(Query query) throws IOException {
        verify(query);

        // strOp.setEntry(new ContentEntry(collection[0]., name));
        strOp.updateCache(query);
        // cacheManager.getSource().getFeatureSource(name)
        return null; //TODO
    }

    @Override
    public boolean isDirty(Query key) throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setDirty(Query key) throws IOException {
        // TODO Auto-generated method stub
        
    }

}
