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

import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.cache.utils.DelegateContentFeatureSource;
import org.geotools.data.cache.utils.EHCacheUtils;
import org.geotools.data.cache.utils.PipelinedContentFeatureReader;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentEntry;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.referencing.CRS;
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
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.springframework.cache.Cache;
import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.cache.support.SimpleValueWrapper;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

public class FeatureSourceOp extends BaseFeatureSourceOp<SimpleFeatureSource, Query, Query> {

    // cached bounds
    protected transient Envelope originalBounds;

    // the cached schema
    protected transient SimpleFeatureType schema;

    // the source schema (needed for query the source)
    protected transient SimpleFeatureType sourceSchema;

    // used to track cached areas
    protected transient Geometry cachedAreas;

    // lock on cached areas
    protected final transient ReadWriteLock lockAreas = new ReentrantReadWriteLock();

    public final static FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2(null);

    protected final Cache ehCacheManager;

    private final static Set<Class<? extends Filter>> supportedFilterTypes = new HashSet<Class<? extends Filter>>(
            Arrays.asList(BBOX.class, Contains.class, Crosses.class, DWithin.class, Equals.class,
                    Intersects.class, Overlaps.class, Touches.class, Within.class));

    public FeatureSourceOp(final CacheManager cacheManager, final String uid) throws IOException {
        super(cacheManager, uid);

        // cache storage
        this.ehCacheManager = EHCacheUtils.getCacheUtils().getCache("featureSource");

        // cached Areas
        initCachedAreas(false);

        // bounds
        initOriginalBounds(null);

        // schema
        // initSchema();
    }

    @Override
    public SimpleFeatureSource getCache(final Query query) throws IOException {
        verify(query);
        return cacheManager.getCache().getFeatureSource(query.getTypeName());
        // return new DelegateContentFeatureSource(cacheManager, getEntry(), query);
    }

    @Override
    public void clear() {
        super.clear();
        initCachedAreas(true);
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

    }

    @Override
    public boolean isCached(Query query) {
        verify(query);
        return isCached(getEnvelope(query.getFilter(), originalBounds));
    }

    @Override
    public void setCached(boolean isCached, Query query) {
        verify(query);
        final Envelope env = getEnvelope(query.getFilter(), originalBounds);
        final Geometry geom = JTS.toGeometry(env);
        try {
            lockAreas.writeLock().lock();
            if (isCached == true) {
                cachedAreas = cachedAreas.union(geom);
            } else {
                cachedAreas = cachedAreas.difference(geom);
            }
        } finally {
            lockAreas.writeLock().unlock();
        }

    }

    /**
     * Updates the cache and modify the passed query filter to the difference from the cached area
     */
    @Override
    public SimpleFeatureSource updateCache(Query query) throws IOException {
        verify(query);
        try {
            if (schema == null || sourceSchema == null) {
                // schema
                if (!initSchema())
                    throw new IllegalStateException("Unable to initialize the schema");
            }
            final Filter[] sF = splitFilters(query, originalBounds, schema);
            final Envelope env = getEnvelope(sF[1], originalBounds);

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
                transform = CRS.findMathTransform(worldCRS, targetCRS != null ? targetCRS
                        : worldCRS);
                // } else if (targetCRS != null) {
                // transform = CRS.findMathTransform(worldCRS != null ? targetCRS : worldCRS, targetCRS);
            }

            final String geoName = geoDesc.getLocalName();
            query.setFilter(ff.intersects(ff.property(geoName),
                    ff.literal(transform != null ? JTS.transform(geom, transform) : geom)));
            query.setProperties(Query.ALL_PROPERTIES);

            if (!isCached(env)) {
                return new DelegateContentFeatureSource(cacheManager, getEntry(), query) {
                    @Override
                    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(
                            Query query) throws IOException {
                        try {
                            lockAreas.writeLock().lock();
                            return new PipelinedContentFeatureReader(getEntry(), query,
                                    cacheManager);
                        } finally {
                            lockAreas.writeLock().unlock();
                        }
                    }
                };
            }

        } catch (Exception e) {
            // TODO LOG
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.severe("Unable to cache the query [" + query + "] due to exception: "
                        + e.getLocalizedMessage());
            }
            throw new IOException(e);
        }

        return null;
    }

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

    private boolean isCached(Envelope envelope) {
        verify(envelope);
        try {
            lockAreas.readLock().lock();
            // cached data?
            if (cachedAreas.contains(JTS.toGeometry(envelope))) {
                return true;
            }
        } finally {
            lockAreas.readLock().unlock();
        }
        return false;
    }

    private static BBOX bboxFilter(Envelope bbox, FeatureType schema) {
        return ff.bbox(schema.getGeometryDescriptor().getLocalName(), bbox.getMinX(),
                bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY(), null);
    }

    private boolean initSchema() throws IOException {
        final ContentEntry entry = getEntry();
        if (entry != null) {
            final Name name = entry.getName();
            sourceSchema = cacheManager.getSource().getSchema(name);
            final SchemaOp schemaOp = cacheManager.getCachedOpOfType(Operation.schema,
                    SchemaOp.class);
            if (schemaOp != null) {
                if (!schemaOp.isCached(name)) {
                    schema = schemaOp.updateCache(name);
                    schemaOp.setCached(schema != null ? true : false, name);
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
     * @param clear if true create and store in cache a new cachedAreas object also updating the current instance
     */
    private void initCachedAreas(final boolean clear) {
        final ValueWrapper vw = ehCacheManager.get(getUid());
        try {
            lockAreas.writeLock().lock();
            if (vw != null && !clear) {
                cachedAreas = (Geometry) vw.get();
            } else {
                cachedAreas = new Polygon(null, null, JTSFactoryFinder.getGeometryFactory());
                ehCacheManager.put(cachedAreas, getUid());
            }
        } finally {
            lockAreas.writeLock().unlock();
        }
    }

    private void initOriginalBounds(final Query query) throws IOException {
        if (query == null) {
            originalBounds = new Envelope(-Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE,
                    Double.MAX_VALUE);
        }

        final CachedOp<Envelope, Query, Query> bOp = cacheManager.getCachedOpOfType(
                Operation.bounds, CachedOp.class);
        Envelope bounds = null;
        if (bOp != null) {
            if (!bOp.isCached(query)) {
                bounds = bOp.updateCache(query);
                bOp.setCached(bounds != null ? true : false, query);
            } else {
                bounds = bOp.getCache(query);
            }
        }
        if (bounds == null) {
            originalBounds = new Envelope(-Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE,
                    Double.MAX_VALUE);
        } else {
            originalBounds = bounds;
        }
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
