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
import java.util.logging.Level;

import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.store.ContentEntry;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.GeometryDescriptor;
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
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

public abstract class BaseFeatureSourceOp<T> extends BaseOp<T, Query> {

    // used to track cached areas
    protected Geometry cachedAreas = new Polygon(null, null, JTSFactoryFinder.getGeometryFactory());

    // lock on cached areas
    protected ReadWriteLock lockCachedAreas = new ReentrantReadWriteLock();

    // used to track dirty areas
    protected Geometry dirtyAreas = new Polygon(null, null, JTSFactoryFinder.getGeometryFactory());

    // lock on dirty areas
    protected ReadWriteLock lockDirtyAreas = new ReentrantReadWriteLock();

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

    private ContentEntry entry;

    public ContentEntry getEntry() {
        return entry;
    }

    public void setEntry(ContentEntry entry) {
        this.entry = entry;
    }

    public BaseFeatureSourceOp(CacheManager cacheManager, final String uid) {
        super(cacheManager, uid);
            originalBounds = new Envelope(-Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE,
                    Double.MAX_VALUE);
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
    protected Query integrateCachedQuery(final Query query) throws IOException {

        final Filter[] sF = splitFilters(query);
        final Envelope env = getEnvelope(sF[1]);
        Geometry geom = JTS.toGeometry(env);
        try {
            lockCachedAreas.readLock().lock();
            geom = geom.difference(cachedAreas.getEnvelope());
        } finally {
            lockCachedAreas.readLock().unlock();
        }
        final Query overQuery;
        final CoordinateReferenceSystem targetCRS = query.getCoordinateSystemReproject();
        final GeometryDescriptor geoDesc = schema.getGeometryDescriptor();
        if (geoDesc != null) {
            final CoordinateReferenceSystem worldCRS = geoDesc.getCoordinateReferenceSystem();
            MathTransform transform = null;
            try {
                if (worldCRS != null) {
                    transform = CRS.findMathTransform(worldCRS, targetCRS != null ? targetCRS
                            : worldCRS);
                    // } else if (targetCRS != null) {
                    // transform = CRS.findMathTransform(worldCRS != null ? targetCRS : worldCRS, targetCRS);
                }
                final String geoName = geoDesc.getLocalName();
                overQuery = new Query(query.getTypeName(), ff.intersects(ff.property(geoName),
                        ff.literal(transform != null ? JTS.transform(geom, transform) : geom)));

            } catch (FactoryException e) {
                throw new IOException(e);
            } catch (MismatchedDimensionException e) {
                throw new IOException(e);
            } catch (TransformException e) {
                throw new IOException(e);
            }
            // updateCache(overQuery);
        } else {
            overQuery = new Query(query);
            overQuery.setProperties(Query.ALL_PROPERTIES);
            // updateCache(overQuery);
        }

        return overQuery;
    }

    @Override
    public boolean isCached(Query query) throws IOException {
        return isSubArea(query);
    }

    protected boolean isSubArea(final Query query) {
        return isSubArea(getEnvelope(query.getFilter()));
    }

    protected boolean isSubArea(final Envelope envelope) {
        return isSubArea(JTS.toGeometry(envelope));
    }

    protected boolean isSubArea(final Geometry geom) {
        try {
            lockCachedAreas.readLock().lock();
            // no cached data?
            if (cachedAreas == null)
                return false;
            return cachedAreas.contains(geom);
        } finally {
            lockCachedAreas.readLock().unlock();
        }
    }

    @Override
    public void setCached(Query query, boolean isCached) throws IOException {
        verify(query);
        if (isCached){
            // integrate cached area with this query
            try {
                lockCachedAreas.writeLock().lock();
                cachedAreas = cachedAreas.union(JTS.toGeometry(getEnvelope(query.getFilter())));
            } finally {
                lockCachedAreas.writeLock().unlock();
            }
        } else {
            // perform a difference between cached area with this query
            try {
                lockCachedAreas.writeLock().lock();
                cachedAreas = cachedAreas.difference(JTS.toGeometry(getEnvelope(query.getFilter())));
            } finally {
                lockCachedAreas.writeLock().unlock();
            }
        }
    }

    @Override
    public boolean isDirty(final Query query) throws IOException {
        return isDirty(getEnvelope(query.getFilter()));
    }

    protected boolean isDirty(final Envelope envelope) throws IOException {
        return isDirty(JTS.toGeometry(envelope));
    }
    
    protected boolean isDirty(Geometry geom) throws IOException {
        try {
            lockDirtyAreas.readLock().lock();
            // no cached data?
            if (dirtyAreas == null)
                return false;
            return dirtyAreas.contains(geom);
        } finally {
            lockDirtyAreas.readLock().unlock();
        }
    }

    @Override
    public void setDirty(Query query) throws IOException {

        final Filter[] sF = splitFilters(query);
        final Envelope env = getEnvelope(sF[1]);
        Geometry geom = JTS.toGeometry(env);
        try {
            lockCachedAreas.readLock().lock();
            cachedAreas = cachedAreas.difference(geom);
        } finally {
            lockCachedAreas.readLock().unlock();
        }

    }

    /**
     * Override this method to clear the features into the cached feature source <br/>
     * NOTE: in the overriding method remember to call super.clear().
     */
    @Override
    public void clear() throws IOException {
        try {
            lockCachedAreas.writeLock().lock();
            cachedAreas = new Polygon(null, null, JTSFactoryFinder.getGeometryFactory());
        } finally {
            lockCachedAreas.writeLock().unlock();
        }
        try {
            lockDirtyAreas.writeLock().lock();
            dirtyAreas = new Polygon(null, null, JTSFactoryFinder.getGeometryFactory());
        } finally {
            lockDirtyAreas.writeLock().unlock();
        }
        FeatureWriter<SimpleFeatureType, SimpleFeature> fw = null;
        try {
            fw = cacheManager.getCache().getFeatureWriter(getEntry().getTypeName(),
                    Transaction.AUTO_COMMIT);
            do {
                fw.remove();
            } while (fw.hasNext());
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, e.getLocalizedMessage(), e);
        } finally {
            if (fw != null) {
                try {
                    fw.close();
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, e.getLocalizedMessage(), e);
                }
            }
        }
        super.clear();
    }

    public Envelope getEnvelope(Filter filter) {
        return getEnvelope(filter, originalBounds);
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

    public static Envelope getEnvelope(final Filter filter, final Envelope originalBounds) {
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

    protected static <T> void ehCachePut(Cache ehCacheManager, T value, Object... keys)
            throws IOException {
        verify(ehCacheManager);
        verify(value);
        verify(keys);

        if (value != null) {
            ehCacheManager.put(Arrays.deepHashCode(keys), value);
        } else {
            throw new IOException(
                    "Unable to cache a null Object, please check the source datastore.");
        }
    }

    protected static <T> T ehCacheGet(Cache cacheManager, Object... keys) {
        verify(cacheManager);
        verify(keys);
        final SimpleValueWrapper vw = (SimpleValueWrapper) cacheManager.get(Arrays
                .deepHashCode(keys));
        if (vw != null) {
            return (T) vw.get();
        } else {
            return null;
        }
    }
}
