package org.geotools.data.cache.impl;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.geotools.data.DataSourceException;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.cache.op.BaseCachedFeatureSource;
import org.geotools.data.cache.op.BaseSchemaOp;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.cache.utils.SimpleFeatureListReader;
import org.geotools.data.store.ContentEntry;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.strtree.STRtree;

@SuppressWarnings("unchecked")
public class STRCachedFeatureSource extends BaseCachedFeatureSource {

    // the cache container
    private STRtree index;

    // lock on cache
    private ReadWriteLock lockIndex = new ReentrantReadWriteLock();

    public STRCachedFeatureSource(CacheManager cacheManager, ContentEntry entry, Query query)
            throws IllegalArgumentException, IOException {
        super(cacheManager, entry, query);
    }

    @Override
    public int getCountInternal(Query query) throws IOException {
        try {
            lockIndex.writeLock().lock();
            if (this.index == null) {
                this.index = new STRtree();
            }

            final Filter[] sF = splitFilters(query);
            final Envelope env = getEnvelope(sF[1]);

            if (this.dirty || !isSubArea(env)) {
                // this.cachedFeatureSource.fillCache(query);
                try {
                    integrateCache(env, query);
                } catch (MismatchedDimensionException e) {
                    throw new IOException(e);
                } catch (FactoryException e) {
                    throw new IOException(e);
                } catch (TransformException e) {
                    throw new IOException(e);
                }
            }
            // new FilteringSimpleFeatureReader(new SimpleListReader(index.query((Envelope)
            // getEnvelope(query.getFilter()),getSchema())),query.getFilter())
            return count(query);

        } catch (Exception e) {
            throw new DataSourceException(
                    "Error occurred extracting features from the spatial index", e);
        } finally {
            lockIndex.writeLock().unlock();
        }

    }

    @Override
    protected int count(Query query) {
        return index.query((Envelope) getEnvelope(query.getFilter())).size();
    }

    @Override
    protected void updateCache(Query query) throws IOException {
        // try to get the schemaOp to use its enrich method (for feature)
        FeatureCollection<SimpleFeatureType, SimpleFeature> features = getDelegate().getFeatures(query);
        FeatureIterator<SimpleFeature> fi = null;
        try {
            lockIndex.writeLock().lock();
            STRtree newIndex = new STRtree();
            fi = features.features();
            while (fi.hasNext()) {
                // consider turning all geometries into packed ones, to save space
                Feature f = fi.next();
                
                // TODO
//                if (schemaOp != null) {
//                    newIndex.insert(ReferencedEnvelope.reference(f.getBounds()),
//                            schemaOp.enrich(f, f));
//                } else {
//                    newIndex.insert(ReferencedEnvelope.reference(f.getBounds()), f);
//                }

            }
            if (!dirty) {
                // fill with old values
                walkSTRtree(newIndex, index.itemsTree());
            }
            index = newIndex;
        } finally {
            if (fi != null) {
                fi.close();
            }
            lockIndex.writeLock().unlock();
        }
    }

    @Override
    protected FeatureReader query(Query query) {
        try {
            lockIndex.writeLock().lock();
            if (this.index == null) {
                this.index = new STRtree();
            }
            List features = index.query((Envelope) getEnvelope(query.getFilter()));

            // return new FilteringFeatureReader(new SimpleListReader(features, getSchema()),
            // query.getFilter());

            return new SimpleFeatureListReader(features, getSchema());
        } finally {
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
