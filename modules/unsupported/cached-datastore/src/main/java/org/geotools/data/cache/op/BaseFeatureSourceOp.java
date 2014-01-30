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
    protected transient ReadWriteLock lockCachedAreas = new ReentrantReadWriteLock();

    // used to track dirty areas
    protected Geometry dirtyAreas = new Polygon(null, null, JTSFactoryFinder.getGeometryFactory());

    // lock on dirty areas
    protected transient ReadWriteLock lockDirtyAreas = new ReentrantReadWriteLock();

    // the cached schema
    protected transient SimpleFeatureType schema;

    protected transient static FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2(null);

    private static final Set<Class<? extends Filter>> supportedFilterTypes = new HashSet<Class<? extends Filter>>(
            Arrays.asList(BBOX.class, Contains.class, Crosses.class, DWithin.class, Equals.class,
                    Intersects.class, Overlaps.class, Touches.class, Within.class));

    private ContentEntry entry;

    public ReadWriteLock getLockCachedAreas() {
        return lockCachedAreas;
    }

    protected Geometry getCachedAreas() {
        return cachedAreas;
    }

    protected void setCachedAreas(Geometry cachedAreas) {
        this.cachedAreas = cachedAreas;
    }

    protected Geometry getDirtyAreas() {
        return dirtyAreas;
    }

    protected void setDirtyAreas(Geometry dirtyAreas) {
        this.dirtyAreas = dirtyAreas;
    }

    protected SimpleFeatureType getSchema() {
        return schema;
    }

    public ContentEntry getEntry() {
        return entry;
    }

    public void setEntry(ContentEntry entry) {
        verify(entry);
        this.entry = entry;
    }

    private CoordinateReferenceSystem worldCRS;

    private GeometryDescriptor geoDesc;

    public void setSchema(SimpleFeatureType schema) {
        if (schema != null) {
            this.schema = schema;
            geoDesc = schema.getGeometryDescriptor();
            if (geoDesc != null) {
                worldCRS = geoDesc.getCoordinateReferenceSystem();
            }
        }
    }

    @Override
    public <E extends CachedOp<T, Query>> void clone(E obj) throws IOException {
        final BaseFeatureSourceOp<T> op = (BaseFeatureSourceOp<T>) obj;
        if (op != null) {
            this.cachedAreas = op.cachedAreas;
            this.lockCachedAreas = op.lockCachedAreas;
            this.dirtyAreas = op.dirtyAreas;
            this.lockDirtyAreas = op.lockDirtyAreas;
            this.setSchema(op.schema);
            this.entry = op.entry;
            super.clone(op);
        }
    }

    public BaseFeatureSourceOp(CacheManager cacheManager, final String uid) throws IOException {
        super(cacheManager, uid);
    }

    public Query queryCachedAreas(final Query query) throws IOException {

        final Query overQuery;

        MathTransform transform = null;
        try {

            overQuery = new Query(query.getTypeName());

            try {
                lockCachedAreas.readLock().lock();
                if (cachedAreas.isEmpty()) {
                    // no geometry in cache
                    overQuery.setFilter(Filter.EXCLUDE);
                } else {// if (cachedAreas.getNumGeometries() > 0) {
                    if (worldCRS != null) {
                        final CoordinateReferenceSystem targetCRS = query
                                .getCoordinateSystemReproject();
                        transform = CRS.findMathTransform(worldCRS, targetCRS != null ? targetCRS
                                : worldCRS);
                    }
                    final String geoName = geoDesc.getLocalName();

                    final Filter areaFilter = ff.intersects(ff.property(geoName), ff
                            .literal(transform != null ? JTS.transform(cachedAreas, transform)
                                    : cachedAreas));
                    final Filter dirtyFilter = ff.intersects(ff.property(geoName), ff
                            .literal(transform != null ? JTS.transform(dirtyAreas, transform)
                                    : dirtyAreas));

                    final Geometry transformedGeom = getGeometry(query);
                    if (transformedGeom != null) {
                        final Filter filter = ff.intersects(ff.property(geoName),
                                ff.literal(transformedGeom));
                        overQuery
                                .setFilter(ff.and(ff.and(filter, areaFilter), ff.not(dirtyFilter)));
                    } else {
                        overQuery.setFilter(ff.and(areaFilter, ff.not(dirtyFilter)));
                    }

                }
            } finally {
                lockCachedAreas.readLock().unlock();
            }
        } catch (FactoryException e) {
            throw new IOException(e);
        } catch (MismatchedDimensionException e) {
            throw new IOException(e);
        } catch (TransformException e) {
            throw new IOException(e);
        }

        overQuery.setProperties(Query.ALL_PROPERTIES);
        return overQuery;
    }

    private Geometry getGeometry(Query query) throws IOException {
        final Filter[] sF = splitFilters(query);
        final Envelope env = getEnvelope(sF[1]);
        if (env.isNull())
            return null;
        final Geometry geom = JTS.toGeometry(env);

        if (schema == null)
            throw new IllegalStateException("You may set the schema before call this method");

        if (geoDesc != null) {
            MathTransform transform = null;
            try {
                if (worldCRS != null) {
                    final CoordinateReferenceSystem targetCRS = query
                            .getCoordinateSystemReproject();
                    transform = CRS.findMathTransform(worldCRS, targetCRS != null ? targetCRS
                            : worldCRS);
                }
                return transform != null ? JTS.transform(geom, transform) : geom;

            } catch (FactoryException e) {
                throw new IOException(e);
            } catch (MismatchedDimensionException e) {
                throw new IOException(e);
            } catch (TransformException e) {
                throw new IOException(e);
            }
        }
        return null;
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
    public Query querySource(final Query query) throws IOException {

        final Query overQuery;

        MathTransform transform = null;
        try {
            if (worldCRS != null) {
                final CoordinateReferenceSystem targetCRS = query.getCoordinateSystemReproject();
                transform = CRS.findMathTransform(worldCRS, targetCRS != null ? targetCRS
                        : worldCRS);
            }
            final String geoName = geoDesc.getLocalName();
            overQuery = new Query(query.getTypeName());

            try {
                lockCachedAreas.readLock().lock();
                if (cachedAreas.isEmpty()) {
                    // no geometry in cache: query for all the geom
                    overQuery.setFilter(query.getFilter());
                } else {// if (cachedAreas.getNumGeometries() > 0) {

                    final Filter areaFilter = ff.intersects(ff.property(geoName), ff
                            .literal(transform != null ? JTS.transform(cachedAreas, transform)
                                    : cachedAreas));
                    final Filter dirtyFilter = ff.intersects(ff.property(geoName), ff
                            .literal(transform != null ? JTS.transform(dirtyAreas, transform)
                                    : dirtyAreas));

                    final Geometry transformedGeom = getGeometry(query);
                    if (transformedGeom != null) {
                        final Filter filter = ff.intersects(ff.property(geoName),
                                ff.literal(transformedGeom));
                        overQuery.setFilter(ff.and(ff.or(filter, dirtyFilter), ff.not(areaFilter)));
                    } else {
                        overQuery.setFilter(ff.and(dirtyFilter, ff.not(areaFilter)));
                    }
                }
            } finally {
                lockCachedAreas.readLock().unlock();
            }
        } catch (FactoryException e) {
            throw new IOException(e);
        } catch (MismatchedDimensionException e) {
            throw new IOException(e);
        } catch (TransformException e) {
            throw new IOException(e);
        }

        overQuery.setProperties(Query.ALL_PROPERTIES);
        return overQuery;
    }

    @Override
    public boolean isCached(Query query) throws IOException {
        final Geometry geom = getGeometry(query);
        if (geom == null) {
            return false;
            // TODO use cache or read from source (how to determine if the cache is complete?)
        }
        try {
            lockCachedAreas.readLock().lock();
            // no cached data?
            if (cachedAreas == null)
                return false;
            return cachedAreas.covers(geom);
        } finally {
            lockCachedAreas.readLock().unlock();
        }
    }

    @Override
    public void setCached(Query query, boolean isCached) throws IOException {
        verify(query);
        final Geometry geom = getGeometry(query);
        if (geom == null) {
            return;
            // TODO use cache or read from source (how to determine if the cache is complete?)
        }

        setCached(geom, isCached);
    }

    public void setCached(Geometry geom, boolean isCached) {
        if (geom == null) {
            return;
        }
        if (isCached) {
            // integrate cached area with this query
            try {
                lockCachedAreas.writeLock().lock();
                cachedAreas = cachedAreas.union(geom);
            } finally {
                lockCachedAreas.writeLock().unlock();
            }
        } else {
            // perform a difference between cached area with this query
            try {
                lockCachedAreas.writeLock().lock();
                cachedAreas = cachedAreas.difference(geom);
            } finally {
                lockCachedAreas.writeLock().unlock();
            }
        }
    }

    @Override
    public boolean isDirty(final Query query) throws IOException {
        final Geometry geom = getGeometry(query);
        if (geom == null) {
            return false;
            // TODO use cache or read from source (how to determine if the cache is complete?)
        }
        try {
            lockDirtyAreas.readLock().lock();
            // no cached data?
            if (dirtyAreas == null)
                return false;
            return dirtyAreas.intersects(geom);
        } finally {
            lockDirtyAreas.readLock().unlock();
        }
    }

    @Override
    public void setDirty(Query query, boolean value) throws IOException {

        final Geometry geom = getGeometry(query);
        if (geom == null) {
            return;
            // TODO use cache or read from source (how to determine if the cache is complete?)
        }
        try {
            lockDirtyAreas.writeLock().lock();
            if (value) {
                dirtyAreas = dirtyAreas.union(geom);
            } else {
                dirtyAreas = dirtyAreas.difference(geom);
            }
        } finally {
            lockDirtyAreas.writeLock().unlock();
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

        // if on this instance has been set the entry we may have written some features, let's remove them
        if (getEntry() != null) {
            FeatureWriter<SimpleFeatureType, SimpleFeature> fw = null;
            try {
                fw = cacheManager.getCache().getFeatureWriter(getEntry().getTypeName(),
                        Transaction.AUTO_COMMIT);
                while (fw.hasNext()) {
                    fw.remove();
                }
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
        }
        super.clear();
    }

    /**
     * Splits a query into two parts, a spatial component that can be turned into a bbox filter (by including some more feature in the result) and a
     * residual component that we cannot address with the spatial index
     * 
     * @param query
     */
    protected Filter[] splitFilters(Query query) {
        return splitFilters(query, schema);
    }

    private static BBOX bboxFilter(Envelope bbox, FeatureType schema) {
        return ff.bbox(schema.getGeometryDescriptor().getLocalName(), bbox.getMinX(),
                bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY(), null);
    }

    /**
     * Splits a query into two parts, a spatial component that can be turned into a bbox filter (by including some more feature in the result) and a
     * residual component that we cannot address with the spatial index
     * 
     * @param query
     */
    protected static Filter[] splitFilters(final Query query, SimpleFeatureType schema) {
        final Filter filter = query.getFilter();

        if (filter == null || filter.equals(Filter.EXCLUDE)) {
            return new Filter[] {
                    Filter.EXCLUDE,
                    bboxFilter(new Envelope(-Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE,
                            Double.MAX_VALUE), schema) };
        }

        if (!(filter instanceof And)) {
            final Envelope envelope = getEnvelope(filter);
            if (envelope == null) {
                return new Filter[] {
                        Filter.EXCLUDE,
                        bboxFilter(new Envelope(-Double.MAX_VALUE, Double.MAX_VALUE,
                                -Double.MAX_VALUE, Double.MAX_VALUE), schema) };
            } else {
                return new Filter[] { Filter.EXCLUDE, bboxFilter(envelope, schema) };
            }
        }

        final And and = (And) filter;
        final List residuals = new ArrayList();
        final List bboxBacked = new ArrayList();
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

    protected static Envelope getEnvelope(final Filter filter) {
        Envelope result = new Envelope();
        if (filter instanceof Or) {
            final Envelope bounds = new Envelope();
            for (Iterator iter = ((Or) filter).getChildren().iterator(); iter.hasNext();) {
                final Filter f = (Filter) iter.next();
                final Envelope e = getEnvelope(f);
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
                final Envelope e = getEnvelope(f);
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
        return result;
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
