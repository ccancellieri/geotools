package org.geotools.data.cache.op.feature.STR;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.CachedOp;
import org.geotools.data.cache.op.feature.BaseFeatureOp;
import org.geotools.data.cache.op.feature.BaseFeatureOpStatus;
import org.geotools.data.cache.utils.DelegateContentFeatureSource;
import org.geotools.data.cache.utils.DelegateSimpleFeature;
import org.geotools.data.cache.utils.SimpleFeatureListReader;
import org.geotools.data.simple.SimpleFeatureReader;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.index.strtree.STRtree;

public class STRFeatureSourceOp {//extends BaseFeatureOp<SimpleFeatureSource> {
//
//    // the cache container
//    private STRtree index = new STRtree();
//
//    // lock on cache
//    private ReadWriteLock lockIndex = new ReentrantReadWriteLock();
//
//    public STRFeatureSourceOp(CacheManager cacheManager, final String uid) throws IOException {
//        super(cacheManager, uid);
//    }
//
//    @Override
//    public <E extends CachedOp<SimpleFeatureSource, Query>> void clone(E obj) throws IOException {
//        if (obj!=null){
//            final STRFeatureSourceOp op = (STRFeatureSourceOp) obj;
//    
//            this.index = op.index;
//            this.lockIndex = op.lockIndex;
//    
//            super.clone(op);
//        }
//    }
//
//    @Override
//    public SimpleFeatureSource updateCache(Query query) throws IOException {
//
//        final Filter[] sF = BaseFeatureOpStatus.splitFilters(query, getSchema());
//        final Envelope env = BaseFeatureOpStatus.getEnvelope(sF[1]);
//        final MathTransform transform = getTransformation(query.getCoordinateSystemReproject());
//        final Geometry queryGeom = getGeometry(env, transform);
//        final String geoName = getGeometryName();
//        final String typeName = query.getTypeName();
//        final Query cacheQuery = queryAreas(typeName, geoName, queryGeom, transform, true, null);
//        final Query sourceQuery = queryAreas(typeName, geoName, queryGeom, transform, false,
//                query.getFilter());
//
//        updateIndex(cacheQuery, sourceQuery);
//
//        return getCache(query);
//    }
//
//    @Override
//    public SimpleFeatureSource getCache(Query query) throws IOException {
//        verify(query);
//
//        final SimpleFeatureSource featureSource = new DelegateContentFeatureSource(cacheManager, query, getStatus()) {
//            @Override
//            protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
//                    throws IOException {
//
//                return new SimpleFeatureListReader(cacheManager, getAbsoluteSchema(),
//                        getListOfFeatures(query));
//            }
//        };
//        return featureSource;
//
//    }
//
//    private List<SimpleFeature> getListOfFeatures(Query query) {
//        try {
//            lockIndex.readLock().lock();
//            return index.query(BaseFeatureOpStatus.getEnvelope(query.getFilter()));
//        } finally {
//            lockIndex.readLock().unlock();
//        }
//    }
//
//    private void updateIndex(Query sourceQuery, Query cachedQuery) throws IOException {
//
//        final SimpleFeatureSource features = new DelegateContentFeatureSource(cacheManager, sourceQuery, getStatus()) {
//            @Override
//            protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(
//                    final Query query) throws IOException {
//
//                return (SimpleFeatureReader) new DelegateSimpleFeature(cacheManager) {
//
//                    final FeatureReader<SimpleFeatureType, SimpleFeature> fi = cacheManager
//                            .getSource().getFeatureReader(query, Transaction.AUTO_COMMIT);
//
//                    @Override
//                    protected Name getFeatureTypeName() {
//                        return getEntry().getName();
//                    }
//
//                    @Override
//                    protected SimpleFeature getNextInternal() throws IllegalArgumentException,
//                            NoSuchElementException, IOException {
//                        final SimpleFeature sf = fi.next();
//                        final SimpleFeature df = SimpleFeatureBuilder.build(getAbsoluteSchema(),
//                                sf.getAttributes(), sf.getID());
//                        return df;
//                    }
//
//                    @Override
//                    public boolean hasNext() throws IOException {
//                        return fi.hasNext();
//                    }
//
//                    @Override
//                    public void close() throws IOException {
//                        if (fi != null)
//                            fi.close();
//                    }
//
//                };
//            }
//        };
//
//        final FeatureIterator<SimpleFeature> fi = features.getFeatures(sourceQuery).features();
//        if (fi.hasNext()) {
//            final STRtree newIndex = new STRtree();
//            do {
//                // consider turning all geometries into packed ones, to save
//                // space
//                final Feature f = fi.next();
//
//                newIndex.insert(ReferencedEnvelope.reference(f.getBounds()), f);
//
//            } while (fi.hasNext());
//            // fill with old values
//            try {
//                lockIndex.readLock().lock();
//                walkSTRtree(newIndex, index.itemsTree(),
//                        BaseFeatureOpStatus.getEnvelope(sourceQuery.getFilter()));
//            } finally {
//                lockIndex.readLock().unlock();
//            }
//            try {
//                lockIndex.writeLock().lock();
//                index = newIndex;
//            } finally {
//                if (fi != null) {
//                    fi.close();
//                }
//                lockIndex.writeLock().unlock();
//            }
//        }
//    }
//
//    private static void walkSTRtree(STRtree dst, Object o, Envelope envelope) {
//        if (o != null) {
//            if (o instanceof Feature) {
//                Feature f = (Feature) o;
//                ReferencedEnvelope env = ReferencedEnvelope.reference(f.getBounds());
//                if (!env.intersects(envelope)) {
//                    dst.insert(env, f);
//                }
//            } else if (o instanceof List) {
//                for (Object oo : ((List) o)) {
//                    walkSTRtree(dst, oo, envelope);
//                }
//            }
//        }
//    }

}
