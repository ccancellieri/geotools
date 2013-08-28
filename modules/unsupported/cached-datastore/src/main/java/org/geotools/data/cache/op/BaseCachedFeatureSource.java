package org.geotools.data.cache.op;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.geotools.data.DataSourceException;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.cache.utils.DelegateContentFeatureSource;
import org.geotools.data.store.ContentEntry;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.referencing.CRS;
import org.opengis.feature.Feature;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.Name;
import org.opengis.filter.And;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.spatial.BinarySpatialOperator;
import org.opengis.filter.spatial.Contains;
import org.opengis.filter.spatial.Crosses;
import org.opengis.filter.spatial.DWithin;
import org.opengis.filter.spatial.Equals;
import org.opengis.filter.spatial.Intersects;
import org.opengis.filter.spatial.Overlaps;
import org.opengis.filter.spatial.Touches;
import org.opengis.filter.spatial.Within;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

@SuppressWarnings("unchecked")
public abstract class BaseCachedFeatureSource extends DelegateContentFeatureSource {

    protected Boolean dirty = true;

    // lock on cache
    protected ReadWriteLock lockCache = new ReentrantReadWriteLock();

    // used to track cached areas
    protected Geometry cachedAreas = new Polygon(null, null, JTSFactoryFinder.getGeometryFactory());

    // lock on cached areas
    protected ReadWriteLock lockAreas = new ReentrantReadWriteLock();

    // cached bounds
    protected final Envelope originalBounds;

    // the cached schema
    // private final SimpleFeatureType schema;

    static FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2(null);

    private static final Set<Class<? extends Filter>> supportedFilterTypes = new HashSet<Class<? extends Filter>>(
            Arrays.asList(BBOX.class, Contains.class, Crosses.class, DWithin.class, Equals.class,
                    Intersects.class, Overlaps.class, Touches.class, Within.class));

    public BaseCachedFeatureSource(CacheManager cacheManager, ContentEntry entry, Query query)
            throws IllegalArgumentException, IOException {
        super(cacheManager, entry, query);
        final Envelope bounds = getDelegate().getBounds();
        if (bounds == null) {
            originalBounds = new Envelope(-Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE,
                    Double.MAX_VALUE);
        } else {
            originalBounds = bounds;
        }

        schema = getSchema();

    }

    protected abstract int count(Query query) throws IOException;

    protected abstract FeatureReader query(Query query) throws IOException;

    // TODO
    // @Override
    // public ReferencedEnvelope getBoundsInternal(Query query) {
    // return ReferencedEnvelope.reference(originalBounds);
    // }

    // TODO
    // @Override
    // public SimpleFeatureType buildFeatureType() throws IOException {
    //
    // final SchemaOp op = (SchemaOp) cacheManager.getCachedOp(Operation.schema);
    // // create schemas
    // if (op != null) {
    // if (op.isCached(entry.getName())) {
    // return op.getCache(entry.getName());
    // } else {
    // SimpleFeatureType schema = op.getCache();
    // op.putCache(schema);
    // return schema;
    // }
    // } else
    // return cacheManager.getSource().getSchema(entry.getName());
    // }

    @Override
    public int getCountInternal(Query query) throws IOException {
        try {
            lockCache.writeLock().lock();

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
            return count(query);

        } catch (Exception e) {
            throw new DataSourceException(
                    "Error occurred extracting features from the spatial index", e);
        } finally {
            lockCache.writeLock().unlock();
        }

    }

    /**
     * integrate the passed index with the passed spatial query (less restrictive than the query itself)
     * 
     * @param query
     * @return
     * @throws IOException
     * @throws FactoryException
     * @throws TransformException
     * @throws MismatchedDimensionException
     */
    protected void integrateCache(final Envelope env, final Query query) throws IOException,
            FactoryException, MismatchedDimensionException, TransformException {

        Geometry geom = JTS.toGeometry(env);
        try {
            lockAreas.readLock().lock();
            geom = geom.difference(cachedAreas.getEnvelope());
        } finally {
            lockAreas.readLock().unlock();
        }

        final CoordinateReferenceSystem targetCRS = query.getCoordinateSystemReproject();
        final GeometryDescriptor geoDesc = schema.getGeometryDescriptor();
        final CoordinateReferenceSystem worldCRS = geoDesc.getCoordinateReferenceSystem();
        MathTransform transform = null;
        if (worldCRS != null) {
            transform = CRS.findMathTransform(worldCRS, targetCRS != null ? targetCRS : worldCRS);
            // } else if (targetCRS != null) {
            // transform = CRS.findMathTransform(worldCRS != null ? targetCRS : worldCRS, targetCRS);
        }
        final String geoName = geoDesc.getLocalName();
        final Query overQuery = new Query(query.getTypeName(), ff.intersects(ff.property(geoName),
                ff.literal(transform != null ? JTS.transform(geom, transform) : geom)));
        updateCache(overQuery);
        try {
            lockAreas.writeLock().lock();
            cachedAreas = cachedAreas.union(geom);
        } finally {
            lockAreas.writeLock().unlock();
        }
        // cachedBounds = cachedAreas.getEnvelopeInternal();
        dirty = false;
    }

    public boolean isDirty() {
        synchronized (dirty) {
            return dirty;
        }

    }

    public void setDirty(boolean dirty) {
        synchronized (this.dirty) {
            this.dirty = dirty;
        }
    }

    protected void updateCache(Query query) throws IOException {
        FeatureCollection<SimpleFeatureType, SimpleFeature> features = getDelegate().getFeatures(
                query);
        // try to get the schemaOp to use its enrich method (for feature)
        final NextOpBkp nextOp = this.cacheManager.getCachedOpOfType(Operation.schema, NextOpBkp.class);
        FeatureIterator<SimpleFeature> fi = null;
        FeatureWriter<SimpleFeatureType, SimpleFeature> fw = null;
        try {
            lockCache.writeLock().lock();
            fi = features.features();
            fw = this.cacheManager.getCache().getFeatureWriterAppend(query.getTypeName(),
                    Transaction.AUTO_COMMIT);
            while (fi.hasNext()) {
                // consider turning all geometries into packed ones, to save space
                Feature sf = fi.next();
                Feature df = fw.next();
                if (nextOp != null) {
                    
                    // TODO
//                    for (Property p : schemaOp.enrich(sf, df)) {
//                        df.getProperty(p.getName()).setValue(p.getValue());
//                    }
//                } else {
//                    for (Property p : sf.getProperties()) {
//                        df.getProperty(p.getName()).setValue(p.getValue());
//                    }
                }
                // df.setDefaultGeometryProperty(sf.getDefaultGeometryProperty());
                fw.write();
            }
        } finally {
            if (fi != null) {
                fi.close();
            }
            if (fw != null) {
                fw.close();
            }
            lockCache.writeLock().unlock();
        }
    }

    protected boolean isSubArea(final Envelope envelope) {
        try {
            lockAreas.readLock().lock();
            // no cached data?
            if (cachedAreas == null)
                return false;
            return cachedAreas.contains(JTS.toGeometry(envelope));
        } finally {
            lockAreas.readLock().unlock();
        }
    }

    protected Envelope getEnvelope(Filter filter) {
        Envelope result = new Envelope();
        if (filter instanceof And) {
            Envelope bounds = new Envelope();
            for (Iterator iter = ((And) filter).getChildren().iterator(); iter.hasNext();) {
                Filter f = (Filter) iter.next();
                Envelope e = getEnvelope(f);
                if (e == null)
                    return null;
                else
                    bounds.expandToInclude(e);
            }
            result = bounds;
        } else if (filter instanceof BinarySpatialOperator) {
            BinarySpatialOperator gf = (BinarySpatialOperator) filter;

            for (Class c : gf.getClass().getInterfaces()) {
                if (supportedFilterTypes.contains(c)) {
                    Expression le = gf.getExpression1();
                    Expression re = gf.getExpression2();
                    if (le instanceof PropertyName && re instanceof Literal) {
                        // String lp = ((PropertyName) le).getPropertyName();
                        Object rl = ((Literal) re).getValue();
                        if (rl instanceof Geometry) {
                            Geometry g = (Geometry) rl;
                            result = g.getEnvelopeInternal();
                        }
                    } else if (le instanceof Literal && re instanceof Literal) {
                        Object ll = ((Literal) le).getValue();
                        Object rl = ((Literal) re).getValue();
                        if (ll instanceof Geometry && rl instanceof PropertyName) {
                            Geometry g = (Geometry) ll;
                            result = g.getEnvelopeInternal();
                        } else if (ll instanceof PropertyName && rl instanceof Geometry) {
                            Geometry g = (Geometry) rl;
                            result = g.getEnvelopeInternal();
                        }
                    }
                    break;
                }
            }
        }
        return result.intersection(originalBounds);
    }

    /**
     * Splits a query into two parts, a spatial component that can be turned into a bbox filter (by including some more feature in the result) and a
     * residual component that we cannot address with the spatial index
     * 
     * @param query
     */
    protected Filter[] splitFilters(Query query) {
        Filter filter = query.getFilter();
        if (filter == null || filter.equals(Filter.EXCLUDE)) {
            return new Filter[] { Filter.EXCLUDE, bboxFilter(originalBounds) };
        }

        if (!(filter instanceof And)) {
            Envelope envelope = getEnvelope(filter);
            if (envelope == null)
                return new Filter[] { Filter.EXCLUDE, bboxFilter(originalBounds) };
            else
                return new Filter[] { Filter.EXCLUDE, bboxFilter(envelope) };
        }

        And and = (And) filter;
        List residuals = new ArrayList();
        List bboxBacked = new ArrayList();
        for (Iterator it = and.getChildren().iterator(); it.hasNext();) {
            Filter child = (Filter) it.next();
            if (getEnvelope(child) != null) {
                bboxBacked.add(child);
            } else {
                residuals.add(child);
            }
        }

        return new Filter[] { (Filter) ff.and(residuals), (Filter) ff.and(bboxBacked) };
    }

    private BBOX bboxFilter(Envelope bbox) {
        return ff.bbox(schema.getGeometryDescriptor().getLocalName(), bbox.getMinX(),
                bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY(), null);
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
            throws IOException {

        try {
            lockCache.writeLock().lock();
            // this.query=query;
            final Filter[] sF = splitFilters(query);
            final Envelope env = getEnvelope(sF[1]);
            if (this.dirty || !isSubArea(env)) {
                // this.cachedFeatureSource.fillCache(query);
                integrateCache(env, query);
            }

            // return new FilteringFeatureReader(new SimpleListReader(features, getSchema()),
            // query.getFilter());

            return query(query);

        } catch (Exception e) {
            throw new IOException("Error occurred extracting features from the spatial index", e);
        } finally {
            lockCache.writeLock().unlock();
        }
    }

}
