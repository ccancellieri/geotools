package org.geotools.data.cache.op.feature;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.geotools.data.Query;
import org.geotools.data.cache.op.CachedOpStatus;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.store.ContentEntry;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
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

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

public class BaseFeatureOpStatus implements CachedOpStatus<Query>, Serializable {

    /** serialVersionUID */
    private static final long serialVersionUID = 1L;

    // used to track cached areas
    protected Geometry cachedAreas = new Polygon(null, null, JTSFactoryFinder.getGeometryFactory());

    // lock on cached areas
    protected final ReadWriteLock lockCachedAreas = new ReentrantReadWriteLock();

    // used to track dirty areas
    protected Geometry dirtyAreas = new Polygon(null, null, JTSFactoryFinder.getGeometryFactory());

    // lock on dirty areas
    protected final ReadWriteLock lockDirtyAreas = new ReentrantReadWriteLock();

    protected static final FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2(null);

    private static final Set<Class<? extends Filter>> supportedFilterTypes = new HashSet<Class<? extends Filter>>(
            Arrays.asList(BBOX.class, Contains.class, Crosses.class, DWithin.class, Equals.class,
                    Intersects.class, Overlaps.class, Touches.class, Within.class));

    // lock on dirty areas
    private final ReadWriteLock lockStatus = new ReentrantReadWriteLock();

    // the cached schema
    protected transient SimpleFeatureType schema;

    private transient CoordinateReferenceSystem worldCRS;

    private transient GeometryDescriptor geoDesc;

    private transient ContentEntry entry;

    // needed to track if the cache is complete if (cachedAreas.covers(originalGeom))
    private transient Geometry originalGeom;

    private transient boolean fullyCached = false;

    public boolean isFullyCached() {
        return fullyCached;
    }

    public SimpleFeatureType getSchema() {
        try {
            lockStatus.readLock().lock();
            return schema;
        } finally {
            lockStatus.readLock().unlock();
        }
    }

    public void setSchema(SimpleFeatureType schema) {
        if (schema != null) {
            try {
                lockStatus.writeLock().lock();
                if (schema != null) {
                    this.schema = schema;
                    geoDesc = schema.getGeometryDescriptor();
                    if (geoDesc != null) {
                        worldCRS = geoDesc.getCoordinateReferenceSystem();
                    }
                }
            } finally {
                lockStatus.writeLock().unlock();
            }
        }
    }

    public ContentEntry getEntry() {
        try {
            lockStatus.readLock().lock();
            return entry;
        } finally {
            lockStatus.readLock().unlock();
        }
    }

    public void setEntry(ContentEntry entry) {
        try {
            lockStatus.writeLock().lock();
            this.entry = entry;
        } finally {
            lockStatus.writeLock().unlock();
        }
    }

    public void setDirty(Geometry geom, boolean isDirty) {
        if (geom == null) {
            // TODO use cache or read from source (how to determine if the cache is complete?)
            return;
        }
        if (isDirty) {
            try {
                lockDirtyAreas.writeLock().lock();
                dirtyAreas = dirtyAreas.union(geom);
            } finally {
                lockDirtyAreas.writeLock().unlock();
            }
        } else {
            // perform a difference between dirty area with this query
            try {
                lockDirtyAreas.writeLock().lock();
                dirtyAreas = dirtyAreas.difference(geom);
            } finally {
                lockDirtyAreas.writeLock().unlock();
            }
        }
    }

    public boolean isDirty(Geometry geom) {
        if (geom == null) {
            return false;
            // TODO check when this is not true!!!!
        }
        try {
            lockDirtyAreas.readLock().lock();
            // no cached data?
            if (dirtyAreas.isEmpty())
                return false;
            return dirtyAreas.intersects(geom);
        } finally {
            lockDirtyAreas.readLock().unlock();
        }
    }

    public Query queryAreas(final String typeName, String geoName, Geometry transformedQueryGeom,
            MathTransform transform, boolean query4Cache, final Filter queryFilter)
            throws IOException {

        final Query overQuery = new Query(typeName);

        try {
            lockCachedAreas.readLock().lock();
            if (cachedAreas.isEmpty() || geoName == null) {
                if (query4Cache) {
                    // no geometry in cache
                    overQuery.setFilter(Filter.EXCLUDE);
                } else {
                    // no geometry in cache: query for all the geom
                    overQuery.setFilter(queryFilter);
                }
            } else {// if (cachedAreas.getNumGeometries() > 0) {
//                if (geoName == null) {
//                    throw new IOException(
//                            "Unable to apply the spatial filter without a geometry name");
//                }
                final Filter areaFilter;
                final Filter dirtyFilter;
                try {
                    areaFilter = ff.contains(ff.property(geoName), ff
                            .literal(transform != null ? JTS.transform(cachedAreas, transform)
                                    : cachedAreas));
                    dirtyFilter = ff.contains(ff.property(geoName), ff
                            .literal(transform != null ? JTS.transform(dirtyAreas, transform)
                                    : dirtyAreas));
                } catch (TransformException e) {
                    throw new IOException(e);
                }
                if (transformedQueryGeom != null) {
                    final Filter filter = ff.intersects(ff.property(geoName),
                            ff.literal(transformedQueryGeom));

                    if (query4Cache) {
                        // query cache
                        overQuery
                                .setFilter(ff.and(ff.and(filter, areaFilter), ff.not(dirtyFilter)));
                    } else {
                        // query source
                        overQuery.setFilter(ff.and(ff.or(filter, dirtyFilter), ff.not(areaFilter)));
                    }
                } else {
                    if (query4Cache) {
                        // query cache
                        overQuery.setFilter(ff.and(areaFilter, ff.not(dirtyFilter)));
                    } else {
                        // query source
                        overQuery.setFilter(ff.and(dirtyFilter, ff.not(areaFilter)));
                    }
                }
            }
        } finally {
            lockCachedAreas.readLock().unlock();
        }

        overQuery.setProperties(Query.ALL_PROPERTIES);
        return overQuery;
    }

    public MathTransform getTransformation(final CoordinateReferenceSystem targetCRS)
            throws IOException {
        if (worldCRS != null) {
            try {
                lockStatus.readLock().lock();
                if (worldCRS != null) {
                    return CRS
                            .findMathTransform(worldCRS, targetCRS != null ? targetCRS : worldCRS);
                }
            } catch (FactoryException e) {
                throw new IOException(e);
            } finally {
                lockStatus.readLock().unlock();
            }
        }
        return null;
    }

    /**
     * Splits a query into two parts, a spatial component that can be turned into a bbox filter (by including some more feature in the result) and a
     * residual component that we cannot address with the spatial index
     * 
     * @param query
     */
//    public Filter[] splitFilters(Query query) {
//        return splitFilters(query, schema);
//    }

//    private static BBOX bboxFilter(Envelope bbox, FeatureType schema) {
//        return ff.bbox(schema.getGeometryDescriptor().getLocalName(), bbox.getMinX(),
//                bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY(), null);
//    }

    /**
     * Splits a query into two parts, a spatial component that can be turned into a bbox filter (by including some more feature in the result) and a
     * residual component that we cannot address with the spatial index
     * 
     * @param query
     */
    // private static Filter[] splitFilters(final Query query, SimpleFeatureType schema) {
    // final Filter filter = query.getFilter();
    //
    // if (filter == null || filter.equals(Filter.EXCLUDE)) {
    // return new Filter[] {
    // Filter.EXCLUDE,
    // bboxFilter(new Envelope(-Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE,
    // Double.MAX_VALUE), schema) };
    // }
    //
    // if (!(filter instanceof And)) {
    // final Envelope envelope = getEnvelope(filter);
    // if (envelope == null) {
    // return new Filter[] {
    // Filter.EXCLUDE,
    // bboxFilter(new Envelope(-Double.MAX_VALUE, Double.MAX_VALUE,
    // -Double.MAX_VALUE, Double.MAX_VALUE), schema) };
    // } else {
    // return new Filter[] { Filter.EXCLUDE, bboxFilter(envelope, schema) };
    // }
    // }
    //
    // final And and = (And) filter;
    // final List residuals = new ArrayList();
    // final List bboxBacked = new ArrayList();
    // for (Iterator it = and.getChildren().iterator(); it.hasNext();) {
    // Filter child = (Filter) it.next();
    // if (getEnvelope(child) != null) {
    // bboxBacked.add(child);
    // } else {
    // residuals.add(child);
    // }
    // }
    // return new Filter[] { (Filter) ff.and(residuals), (Filter) ff.and(bboxBacked) };
    // }

    public static Envelope getEnvelope(final Filter filter) {
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
                            result.expandToInclude(g.getEnvelopeInternal());
                        }
                    } else if (le instanceof Literal && re instanceof Literal) {
                        final Object ll = ((Literal) le).getValue();
                        final Object rl = ((Literal) re).getValue();
                        if (ll instanceof Geometry && rl instanceof PropertyName) {
                            final Geometry g = (Geometry) ll;
                            result.expandToInclude(g.getEnvelopeInternal());
                        } else if (ll instanceof PropertyName && rl instanceof Geometry) {
                            final Geometry g = (Geometry) rl;
                            result.expandToInclude(g.getEnvelopeInternal());
                        }
                    }
                    break;
                }
            }
        }
        return result;
    }

    public void setOriginalGeometry(Geometry envelope) {
        this.originalGeom = envelope;
    }

    public String getGeometryName() {
        if (geoDesc != null) {
            try {
                lockStatus.readLock().lock();
                if (geoDesc != null) {
                    return geoDesc.getLocalName();
                }
            } finally {
                lockStatus.readLock().unlock();
            }
        }
        return null;
    }

    public Geometry getGeometry(Query query) throws IOException {
//        final Filter[] sF = splitFilters(query);
        final Envelope env = getEnvelope(query.getFilter());
        if (env == null || env.isNull())
            return null;
        final MathTransform transform = getTransformation(query.getCoordinateSystemReproject());
        return getGeometry(env, transform);
    }

    public static Geometry getGeometry(Envelope env, MathTransform transform) throws IOException {
        if (env == null || env.isNull())
            return null;
        final Geometry geom = JTS.toGeometry(env);

        try {
            return transform != null ? JTS.transform(geom, transform) : geom;
        } catch (MismatchedDimensionException e) {
            throw new IOException(e);
        } catch (TransformException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean isCached(Query query) throws IOException {
        if (fullyCached)
            return true;
//        final Filter[] sF = splitFilters(query);
        final Envelope env = getEnvelope(query.getFilter());
        if (env == null || env.isNull()) {
            return false;
            // TODO use cache or read from source (how to determine if the cache is complete?)
        }
        final MathTransform transform = getTransformation(query.getCoordinateSystemReproject());
        final Geometry geom = getGeometry(env, transform);
        if (geom == null) {
            return false;
            // TODO use cache or read from source (how to determine if the cache is complete?)
        }
        return isCached(geom);
    }

    private boolean isCached(Geometry geom) throws IOException {
        if (fullyCached) {
            return true;
        } else if (geom == null) {
            // you are asking for all the geometries in the source
            // geom=originalGeom; may returns always false otherwise fullyCached should be true
            return false;
        }
        try {
            lockCachedAreas.readLock().lock();
            // no cached data?
            if (cachedAreas == null || cachedAreas.isEmpty())
                return false;
            return cachedAreas.covers(geom);
        } finally {
            lockCachedAreas.readLock().unlock();
        }
    }

    @Override
    public void setCached(Query query, boolean isCached) throws IOException {
        final Geometry geom = getGeometry(query);

        setCached(geom, isCached);
    }

    public void setCached(Geometry geom, boolean isCached) throws IOException {
        if (geom == null) {
            if (isCached) {
                // set ALL as cached
                try {
                    lockCachedAreas.writeLock().lock();
                    cachedAreas = cachedAreas.union(originalGeom);
                } finally {
                    lockCachedAreas.writeLock().unlock();
                }
            } else {
                // set Nothing as cached
                fullyCached = false;
                try {
                    lockCachedAreas.writeLock().lock();
                    cachedAreas = cachedAreas.difference(originalGeom);
                } finally {
                    lockCachedAreas.writeLock().unlock();
                }
            }
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
        try {
            lockStatus.writeLock().lock();
            if (cachedAreas.covers(originalGeom)) {
                fullyCached = true;
            } else {
                fullyCached = false;
            }
        } finally {
            lockStatus.writeLock().unlock();
        }
    }

    @Override
    public boolean isDirty(final Query query) throws IOException {
        return isDirty(getGeometry(query));
    }

    @Override
    public void setDirty(Query query, boolean value) throws IOException {

        final Geometry geom = getGeometry(query);
        if (geom == null) {
            return;
            // TODO use cache or read from source (how to determine if the cache is complete?)
        }
        setDirty(geom, value);
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
        try {
            lockStatus.writeLock().lock();
            fullyCached=false;
        } finally {
            lockStatus.writeLock().unlock();
        }
        
    }

    private final static Operation[] applicableOperations = new Operation[] {
            Operation.featureCount, Operation.featureCollection, Operation.featureReader,
            Operation.featureSource };
    static {
        Arrays.sort(applicableOperations);
    }

    @Override
    public boolean isApplicable(Operation op) {
        if (Arrays.binarySearch(applicableOperations, op) > 0) {
            return true;
        }
        return false;
    }

    public Geometry getOriginalGeometry() {
        return originalGeom;
    }

}
