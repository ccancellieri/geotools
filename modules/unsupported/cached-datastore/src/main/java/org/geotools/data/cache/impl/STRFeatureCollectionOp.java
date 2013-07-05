package org.geotools.data.cache.impl;

import java.io.IOException;

import org.geotools.data.cache.op.BaseOp;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.feature.NameImpl;

public class STRFeatureCollectionOp extends BaseOp<SimpleFeatureCollection, String, String> {

    STRFeatureSourceOp strOp ;
    
    public STRFeatureCollectionOp(CacheManager cacheManager, final String uniqueName) {
        super(cacheManager,uniqueName);
        strOp = new STRFeatureSourceOp(cacheManager, getUid());
    }

    @Override
    public SimpleFeatureCollection getCache(String... typeName) throws IOException {
            strOp.setEntry(new ContentEntry((ContentDataStore) cacheManager.getSource(), new NameImpl(typeName[0])));
            SimpleFeatureSource s = strOp.getCache(typeName);
            return s.getFeatures();
    }

    @Override
    public boolean putCache(SimpleFeatureCollection... collection) throws IOException {
        strOp.putCache(cacheManager.getSource().getFeatureSource(strOp.getEntry().getTypeName()));
        return true;
    }


}
