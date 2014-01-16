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
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.Name;
import org.opengis.filter.And;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.Or;
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
import org.springframework.cache.Cache.ValueWrapper;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

@SuppressWarnings("unchecked")
public abstract class BaseCachedFeatureSource extends DelegateContentFeatureSource {

    private Boolean dirty = true;

    // used to track cached areas
    protected Geometry cachedAreas = new Polygon(null, null, JTSFactoryFinder.getGeometryFactory());

    // lock on cached areas
    protected ReadWriteLock lockAreas = new ReentrantReadWriteLock();

    // cached bounds
    protected transient Envelope originalBounds;

    // the cached schema
    protected transient SimpleFeatureType schema;

    // the source schema (needed for query the source)
    protected transient SimpleFeatureType sourceSchema;

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
        // final CachedOp<Envelope, Query, Query> bOp = cacheManager.getCachedOpOfType(
        // Operation.bounds, CachedOp.class);
        // Envelope bounds = null;
        // if (bOp != null) {
        // if (!bOp.isCached(query)) {
        // bounds = bOp.updateCache(query);
        // bOp.setCached(bounds != null ? true : false, query);
        // } else {
        // bounds = bOp.getCache(query);
        // }
        // }
        // if (bounds == null) {
        // originalBounds = new Envelope(-Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE,
        // Double.MAX_VALUE);
        // } else {
        // originalBounds = bounds;
        // }
        if (bounds == null) {
            originalBounds = new Envelope(-Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE,
                    Double.MAX_VALUE);
        } else {
            originalBounds = bounds;
        }

        clearCachedAreas();

        initSchema();

    }

    void clearCachedAreas() {
        try {
            lockAreas.writeLock().lock();
            cachedAreas = new Polygon(null, null, JTSFactoryFinder.getGeometryFactory());
        } finally {
            lockAreas.writeLock().unlock();
        }
    }

    protected boolean initSchema() throws IOException {
        final ContentEntry entry = getEntry();
        if (entry != null) {
            final Name name = entry.getName();
            sourceSchema = cacheManager.getSource().getSchema(name);
            final SchemaOp schemaOp = cacheManager.getCachedOpOfType(Operation.schema,
                    SchemaOp.class);
            if (schemaOp != null) {
                if (!schemaOp.isCached(cacheManager.getUID())) {
                    schema = schemaOp.updateCache(name);
                    schemaOp.setCached(schema != null ? true : false, cacheManager.getUID());
                } else {
                    schema = schemaOp.getCache(getEntry().getName());
                }
            } else {
                schema = sourceSchema;
            }
            return true;
        }
        return false;
    }

    /**
     * Override this method if you want to modify standard (delegation) behavior
     * 
     * @param query
     * @return
     * @throws IOException
     */
    protected int count(Query origQuery, Query integratedQuery) throws IOException {
        return super.getCountInternal(origQuery);
    }

    /**
     * Override this method if you want to modify standard (delegation) behavior
     * 
     * @param query
     * @return
     * @throws IOException
     */
    protected FeatureReader<SimpleFeatureType, SimpleFeature> query(Query origQuery, Query integratedQuery) throws IOException {
        return super.getReaderInternal(origQuery);
    }

    // TODO
    // @Override
    // public ReferencedEnvelope getBoundsInternal(Query query) {
    // return ReferencedEnvelope.reference(originalBounds);
    // }

    @Override
    public int getCountInternal(Query query) throws IOException {

        if (isDirty() || !isSubArea(query)) {
            // this.cachedFeatureSource.fillCache(query);
            try {
                return count(query,integrateCachedQuery(query));
            } catch (MismatchedDimensionException e) {
                throw new IOException(e);
            } catch (FactoryException e) {
                throw new IOException(e);
            } catch (TransformException e) {
                throw new IOException(e);
            }
        }
        return count(query,query);

    }

    /**
     * integrate the passed query with the missing portion of areas to query and returns the modified query (with all attributes as properties and the
     * difference between areas as spatial query).
     * 
     * @param query
     * @return
     * @throws IOException
     * @throws FactoryException
     * @throws TransformException
     * @throws MismatchedDimensionException
     */
    protected Query integrateCachedQuery(final Query query) throws IOException,
            FactoryException, MismatchedDimensionException, TransformException {
        
        final Filter[] sF = splitFilters(query);
        final Envelope env = getEnvelope(sF[1]);
        Geometry geom = JTS.toGeometry(env);
        try {
            lockAreas.readLock().lock();
            geom = geom.difference(cachedAreas.getEnvelope());
        } finally {
            lockAreas.readLock().unlock();
        }
        final Query overQuery;
        final CoordinateReferenceSystem targetCRS = query.getCoordinateSystemReproject();
        final GeometryDescriptor geoDesc = schema.getGeometryDescriptor();
        if (geoDesc != null) {
            final CoordinateReferenceSystem worldCRS = geoDesc.getCoordinateReferenceSystem();
            MathTransform transform = null;
            if (worldCRS != null) {
                transform = CRS.findMathTransform(worldCRS, targetCRS != null ? targetCRS
                        : worldCRS);
                // } else if (targetCRS != null) {
                // transform = CRS.findMathTransform(worldCRS != null ? targetCRS : worldCRS, targetCRS);
            }
            final String geoName = geoDesc.getLocalName();
            overQuery = new Query(query.getTypeName(), ff.intersects(ff.property(geoName),
                    ff.literal(transform != null ? JTS.transform(geom, transform) : geom)));
            // updateCache(overQuery);
        } else {
            overQuery = new Query(query);
            overQuery.setProperties(Query.ALL_PROPERTIES);
            // updateCache(overQuery);
        }
        try {
            lockAreas.writeLock().lock();
            cachedAreas = cachedAreas.union(geom);
        } finally {
            lockAreas.writeLock().unlock();
        }
        // cachedBounds = cachedAreas.getEnvelopeInternal();
        setDirty(false);

        return overQuery;
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

    // protected void updateCache(Query query) throws IOException {
    // FeatureCollection<SimpleFeatureType, SimpleFeature> features = getDelegate().getFeatures(
    // query);
    // // try to get the schemaOp to use its enrich method (for feature)
    // final SchemaOp schemaOp = this.cacheManager.getCachedOpOfType(Operation.schema,
    // SchemaOp.class);
    // final NextOp nextOp = this.cacheManager.getCachedOpOfType(Operation.next, NextOp.class);
    // FeatureIterator<SimpleFeature> fi = null;
    // FeatureWriter<SimpleFeatureType, SimpleFeature> fw = null;
    // try {
    // lockCache.writeLock().lock();
    // fi = features.features();
    // fw = this.cacheManager.getCache().getFeatureWriterAppend(query.getTypeName(),
    // Transaction.AUTO_COMMIT);
    // while (fi.hasNext()) {
    // // consider turning all geometries into packed ones, to save space
    // Feature sf = fi.next();
    // Feature df = fw.next();
    // if (nextOp != null) {
    //
    // // TODO
    // if (schemaOp != null) {
    // // for (Property p : schemaOp.enrich(sf, df)) {
    // // df.getProperty(p.getName()).setValue(p.getValue());
    // // }
    // // } else {
    // // for (Property p : sf.getProperties()) {
    // // df.getProperty(p.getName()).setValue(p.getValue());
    // // }
    // }
    // }
    // // df.setDefaultGeometryProperty(sf.getDefaultGeometryProperty());
    // fw.write();
    // }
    // } finally {
    // if (fi != null) {
    // fi.close();
    // }
    // if (fw != null) {
    // fw.close();
    // }
    // lockCache.writeLock().unlock();
    // }
    // }
    
    protected boolean isSubArea(final Query envelope) {
        return isSubArea(getEnvelope(query.getFilter()));
    }

    protected boolean isSubArea(final Envelope envelope) {
        return isSubArea(JTS.toGeometry(envelope));
    }

    protected boolean isSubArea(final Geometry geom) {
        try {
            lockAreas.readLock().lock();
            // no cached data?
            if (cachedAreas == null)
                return false;
            return cachedAreas.contains(geom);
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

    /**
     * Splits a query into two parts, a spatial component that can be turned into a bbox filter (by including some more feature in the result) and a
     * residual component that we cannot address with the spatial index
     * 
     * @param query
     */
    protected static Filter[] splitFilters(final Query query, final Envelope originalBounds,
            SimpleFeatureType schema) {
        final Filter filter = query.getFilter();
        if (filter == null || filter.equals(Filter.EXCLUDE)) {
            return new Filter[] { Filter.EXCLUDE, bboxFilter(originalBounds, schema) };
        }

        if (!(filter instanceof And)) {
            final Envelope envelope = getEnvelope(filter, originalBounds);
            if (envelope == null)
                return new Filter[] { Filter.EXCLUDE, bboxFilter(originalBounds, schema) };
            else
                return new Filter[] { Filter.EXCLUDE, bboxFilter(envelope, schema) };
        }

        final And and = (And) filter;
        final List residuals = new ArrayList();
        final List bboxBacked = new ArrayList();
        for (Iterator it = and.getChildren().iterator(); it.hasNext();) {
            Filter child = (Filter) it.next();
            if (getEnvelope(child, originalBounds) != null) {
                bboxBacked.add(child);
            } else {
                residuals.add(child);
            }
        }
        return new Filter[] { (Filter) ff.and(residuals), (Filter) ff.and(bboxBacked) };
    }

    protected static Envelope getEnvelope(final Filter filter, final Envelope originalBounds) {
        Envelope result = new Envelope();
        if (filter instanceof Or) {
            final Envelope bounds = new Envelope();
            for (Iterator iter = ((Or) filter).getChildren().iterator(); iter.hasNext();) {
                final Filter f = (Filter) iter.next();
                final Envelope e = getEnvelope(f, originalBounds);
                if (e == null)
                    return null;
                else
                    bounds.expandToInclude(e);
            }
            result = bounds;
        } else if (filter instanceof And) {
            final Envelope bounds = new Envelope();
            for (Iterator iter = ((And) filter).getChildren().iterator(); iter.hasNext();) {
                final Filter f = (Filter) iter.next();
                final Envelope e = getEnvelope(f, originalBounds);
                if (e == null)
                    return null;
                else
                    bounds.expandToInclude(e);
            }
            result = bounds;
        } else if (filter instanceof BinarySpatialOperator) {
            final BinarySpatialOperator gf = (BinarySpatialOperator) filter;

            for (Class c : gf.getClass().getInterfaces()) {
                if (supportedFilterTypes.contains(c)) {
                    final Expression le = gf.getExpression1();
                    final Expression re = gf.getExpression2();
                    if (le instanceof PropertyName && re instanceof Literal) {
                        // String lp = ((PropertyName) le).getPropertyName();
                        final Object rl = ((Literal) re).getValue();
                        if (rl instanceof Geometry) {
                            Geometry g = (Geometry) rl;
                            result = g.getEnvelopeInternal();
                        }
                    } else if (le instanceof Literal && re instanceof Literal) {
                        final Object ll = ((Literal) le).getValue();
                        final Object rl = ((Literal) re).getValue();
                        if (ll instanceof Geometry && rl instanceof PropertyName) {
                            final Geometry g = (Geometry) ll;
                            result = g.getEnvelopeInternal();
                        } else if (ll instanceof PropertyName && rl instanceof Geometry) {
                            final Geometry g = (Geometry) rl;
                            result = g.getEnvelopeInternal();
                        }
                    }
                    break;
                }
            }
        }
        return result.intersection(originalBounds);
    }

    private static BBOX bboxFilter(Envelope bbox, FeatureType schema) {
        return ff.bbox(schema.getGeometryDescriptor().getLocalName(), bbox.getMinX(),
                bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY(), null);
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
            throws IOException {

        if (isDirty() || !isSubArea(query)) {
            // this.cachedFeatureSource.fillCache(query);
            try {
                return query(query,integrateCachedQuery(query));
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        return query(query,query);

    }

}
