package org.geotools.data.cache.impl;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.cache.op.BaseFeatureSourceOp;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.cache.utils.DelegateContentFeatureSource;
import org.geotools.data.cache.utils.PipelinedContentFeatureReader;
import org.geotools.data.cache.utils.SimpleFeatureListReader;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureIterator;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.strtree.STRtree;

public class STRFeatureSourceOp extends BaseFeatureSourceOp<SimpleFeatureSource> {

    // the cache container
    private STRtree index = new STRtree();

    // lock on cache
    private ReadWriteLock lockIndex = new ReentrantReadWriteLock();

    public STRFeatureSourceOp(CacheManager cacheManager, final String uid) throws IOException {
        super(cacheManager, uid);
    }

    @Override
    public SimpleFeatureSource updateCache(Query query) throws IOException {
        updateIndex(super.integrateCachedQuery(query));

        return getCache(query);
    }

    @Override
    public SimpleFeatureSource getCache(Query query) throws IOException {
        verify(query);

        final SimpleFeatureSource featureSource = new DelegateContentFeatureSource(cacheManager,
                getEntry(), query) {
            @Override
            protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
                    throws IOException {

                return new SimpleFeatureListReader(getListOfFeatures(query), schema);
            }
        };
        return featureSource;

    }

    private List<SimpleFeature> getListOfFeatures(Query query) {
        try {
            lockIndex.readLock().lock();
            return index.query((Envelope) BaseFeatureSourceOp.getEnvelope(query.getFilter(),
                    new Envelope(-Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE,
                            Double.MAX_VALUE)));
        } finally {
            lockIndex.readLock().unlock();
        }
    }

    private void updateIndex(Query query) throws IOException {
        // try to get the schemaOp to use its enrich method (for feature)
        SimpleFeatureSource features = cacheManager.getSource().getFeatureSource(
                query.getTypeName());
        FeatureIterator<SimpleFeature> fi = null;

        STRtree newIndex = new STRtree();
        fi = features.getFeatures(query).features();
        boolean dirty = false;
        if (fi.hasNext()) {
            dirty = true;
        }
        while (fi.hasNext()) {
            // consider turning all geometries into packed ones, to save space
            Feature f = fi.next();

            newIndex.insert(ReferencedEnvelope.reference(f.getBounds()), f);

        }
        if (dirty) {
            // fill with old values
            try {
                lockIndex.readLock().lock();
                walkSTRtree(newIndex, index.itemsTree());
            } finally {
                lockIndex.readLock().unlock();
            }

        }
        try {
            lockIndex.writeLock().lock();
            index = newIndex;
        } finally {
            if (fi != null) {
                fi.close();
            }
            lockIndex.writeLock().unlock();
        }
    }

    private static void walkSTRtree(STRtree dst, Object o) {
        if (o != null) {
            if (o instanceof Feature) {
                Feature f = (Feature) o;
                dst.insert(ReferencedEnvelope.reference(f.getBounds()), f);
            } else if (o instanceof List) {
                for (Object oo : ((List) o)) {
                    walkSTRtree(dst, oo);
                }
            }
        }
    }

}
