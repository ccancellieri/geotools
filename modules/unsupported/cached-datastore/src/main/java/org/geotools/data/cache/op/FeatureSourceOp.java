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

import org.geotools.data.DataSourceException;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.cache.impl.STRCachedFeatureSource;
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

public class FeatureSourceOp extends BaseFeatureSourceOp<BaseCachedFeatureSource, Query, String> {

    // // used to track cached areas
    // protected transient Geometry cachedAreas;

    // // lock on cached areas
    // protected final transient ReadWriteLock lockAreas = new ReentrantReadWriteLock();

    // public final static FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2(null);

    protected final Cache ehCacheManager;

    // lock on cache
    protected ReadWriteLock lockCache = new ReentrantReadWriteLock();


    // private final static Set<Class<? extends Filter>> supportedFilterTypes = new HashSet<Class<? extends Filter>>(
    // Arrays.asList(BBOX.class, Contains.class, Crosses.class, DWithin.class, Equals.class,
    // Intersects.class, Overlaps.class, Touches.class, Within.class));

    public FeatureSourceOp(final CacheManager cacheManager, final String uid) throws IOException {
        super(cacheManager, uid);

        // cache storage
        this.ehCacheManager = EHCacheUtils.getCacheUtils().getCache("featureSource");

        // cached Areas
        // initCachedAreas(false);

        // bounds
        // initOriginalBounds(null);

        // schema
        // initSchema();
    }

    @Override
    public BaseCachedFeatureSource getCache(Query query) throws IOException {
        BaseCachedFeatureSource s = ehCacheGet(ehCacheManager, this.getUid());
        if (s == null) {
            s = updateCache(query);
            setCached(s != null ? true : false, this.getUid());
            return getCache(query);
        } else {
            return s;
        }
    }

    @Override
    public BaseCachedFeatureSource updateCache(Query query) throws IOException {
        final BaseCachedFeatureSource featureSource;
        try {
            lockCache.writeLock().lock();
            featureSource = new BaseCachedFeatureSource(cacheManager, getEntry(), query) {
                @Override
                protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(
                        Query query) throws IOException {

                    return new PipelinedContentFeatureReader(getEntry(), query, cacheManager);

                }
            };
            ehCachePut(ehCacheManager, featureSource, this.getUid());
        } finally {
            lockCache.writeLock().unlock();
        }
        return featureSource;
    }

    @Override
    public void clear() throws IOException {
        final BaseCachedFeatureSource featureSource=getCache(null);
        if (featureSource!=null){
            featureSource.clearCachedAreas();
            featureSource.initSchema();
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

//    @Override
//    public boolean isCached(String query) {
//        verify(query);
//        return !featureSource.isDirty()
//                && featureSource.isSubArea(query);
//        // isCached(getEnvelope(query.getFilter(), originalBounds));
//    }

//    @Override
//    public void setCached(boolean isCached, String UUID) {
//        verify(UUID);
//        super.set
//
//    }

    /**
     * Updates the cache and modify the passed query filter to the difference from the cached area
     */
    // @Override
    // public SimpleFeatureSource updateCache(Query query) throws IOException {
    // verify(query);
    // try {
    // if (schema == null || sourceSchema == null) {
    // // schema
    // if (!initSchema())
    // throw new IllegalStateException("Unable to initialize the schema");
    // }
    // final Filter[] sF = splitFilters(query, originalBounds, schema);
    // final Envelope env = getEnvelope(sF[1], originalBounds);
    //
    // Geometry geom = JTS.toGeometry(env);
    // try {
    // lockAreas.readLock().lock();
    // geom = geom.difference(cachedAreas.getEnvelope());
    // } finally {
    // lockAreas.readLock().unlock();
    // }
    //
    // final CoordinateReferenceSystem targetCRS = query.getCoordinateSystemReproject();
    // final GeometryDescriptor geoDesc = schema.getGeometryDescriptor();
    // final CoordinateReferenceSystem worldCRS = geoDesc.getCoordinateReferenceSystem();
    // MathTransform transform = null;
    // if (worldCRS != null) {
    // transform = CRS.findMathTransform(worldCRS, targetCRS != null ? targetCRS
    // : worldCRS);
    // // } else if (targetCRS != null) {
    // // transform = CRS.findMathTransform(worldCRS != null ? targetCRS : worldCRS, targetCRS);
    // }
    //
    // final String geoName = geoDesc.getLocalName();
    // query.setFilter(ff.intersects(ff.property(geoName),
    // ff.literal(transform != null ? JTS.transform(geom, transform) : geom)));
    // query.setProperties(Query.ALL_PROPERTIES);
    //
    // if (!isCached(env)) {
    // return new BaseCachedFeatureSource(cacheManager, getEntry(), query) {
    // @Override
    // protected FeatureReader<SimpleFeatureType, SimpleFeature> query(Query query)
    // throws IOException {
    // try {
    // lockAreas.writeLock().lock();
    // return new PipelinedContentFeatureReader(getEntry(), query,
    // cacheManager);
    // } finally {
    // lockAreas.writeLock().unlock();
    // }
    // }
    // };
    // }
    //
    // } catch (Exception e) {
    // // TODO LOG
    // if (LOGGER.isLoggable(Level.SEVERE)) {
    // LOGGER.severe("Unable to cache the query [" + query + "] due to exception: "
    // + e.getLocalizedMessage());
    // }
    // throw new IOException(e);
    // }
    //
    // return null;
    // }

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
