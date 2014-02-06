package org.geotools.data.cache.op.feature;

import java.io.IOException;
import java.util.logging.Logger;

import org.geotools.data.Query;
import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.BaseOp;
import org.geotools.data.cache.op.CachedOpStatus;

public abstract class BaseFeatureOp<T> extends BaseOp<Query, T> {
    protected final transient Logger LOGGER = org.geotools.util.logging.Logging
            .getLogger(getClass().getPackage().getName());


    public BaseFeatureOp(CacheManager cacheManager, final CachedOpStatus<Query> status)
            throws IOException {
        super(cacheManager, status);
    }
//
//    public MathTransform getTransformation(CoordinateReferenceSystem crs) throws IOException {
//        if (crs == null)
//            return null;
//        if (status == null) {
//            throw new IllegalStateException("Status is null");
//        }
//        return status.getTransformation(crs);
//    }
//
//    public String getGeometryName() {
//        if (status == null) {
//            throw new IllegalStateException("Status is null");
//        }
//        return status.getGeometryName();
//
//    }
//
//    public Geometry getGeometry(Envelope env, MathTransform transform) throws IOException {
//        // if (env==null || transform==null)
//        // return null;
//        if (status == null) {
//            throw new IllegalStateException("Status is null");
//        }
//        return status.getGeometry(env, transform);
//
//    }
//
//    public boolean isCached(Geometry geom) throws IOException {
//        if (status == null) {
//            throw new IllegalStateException("Status is null");
//        }
//        return status.isCached(geom);
//    }
//
//    public void setCached(Geometry geom, boolean isCached) {
//        if (geom == null)
//            return;
//        if (status == null) {
//            throw new IllegalStateException("Status is null");
//        }
//        status.setCached(geom, isCached);
//    }
//
//    public void setDirty(Geometry geom, boolean isDirty) {
//        status.setDirty(geom, isDirty);
//    }
//
//    public Query queryAreas(String typeName, String geoName, Geometry transformedQueryGeom,
//            MathTransform transform, boolean query4Cache, Filter queryFilter) throws IOException {
//        if (status == null) {
//            throw new IllegalStateException("Status is null");
//        }
//        return status.queryAreas(typeName, geoName, transformedQueryGeom, transform, query4Cache,
//                queryFilter);
//    }
//
//    public boolean isDirty(Geometry geom) {
//        if (geom == null) {
//            return false;
//        }
//        if (status == null) {
//            throw new IllegalStateException("Status is null");
//        }
//        return status.isDirty(geom);
//    }
//
//    public Geometry getGeometry(Query query) throws IOException {
//        if (query == null)
//            return null;
//        if (status == null) {
//            throw new IllegalStateException("Status is null");
//        }
//        return status.getGeometry(query);
//    }
//
//    public FeatureStatus getStatus() {
//        return status;
//    }
//
//    public void setStatus(FeatureStatus status) {
//        this.status = status;
//    }
//
//    public SimpleFeatureType getSchema() {
//        if (status == null) {
//            throw new IllegalStateException("Status is null");
//        }
//        return status.getSchema();
//    }
//
//    public void setSchema(SimpleFeatureType schema) {
//        if (status == null) {
//            throw new IllegalStateException("Status is null");
//        }
//        status.setSchema(schema);
//    }
//
//    public ContentEntry getEntry() {
//        if (status == null) {
//            throw new IllegalStateException("Status is null");
//        }
//        return status.getEntry();
//    }
//
//    public void setEntry(ContentEntry entry) {
//        if (status == null) {
//            throw new IllegalStateException("Status is null");
//        }
//        status.setEntry(entry);
//    }
//
//    @Override
//    public <E extends CachedOp<T, Query>> void clone(E obj) throws IOException {
//        if (obj != null) {
//            final BaseFeatureOp<T> op = (BaseFeatureOp<T>) obj;
//            super.clone(op);
//            if (status != null) {
//                this.status.clone(op.status);
//            }
//        }
//    }

//    @Override
//    public boolean isCached(Query query) throws IOException {
//        if (status == null) {
//            throw new IllegalStateException("Status is null");
//        }
//        final Geometry geom = status.getGeometry(query);
//        if (geom == null) {
//            return false;
//            // TODO use cache or read from source (how to determine if the cache is complete?)
//        }
//        return status.isCached(geom);
//    }
//
//    @Override
//    public void setCached(Query query, boolean isCached) throws IOException {
//        if (status == null) {
//            throw new IllegalStateException("Status is null");
//        }
////        verify(query);
//        final Geometry geom = status.getGeometry(query);
//        if (geom == null) {
//            return;
//            // TODO use cache or read from source (how to determine if the cache is complete?)
//        }
//        status.setCached(geom, isCached);
//    }
//
//    @Override
//    public boolean isDirty(final Query query) throws IOException {
//        if (status == null) {
//            throw new IllegalStateException("Status is null");
//        }
//        final Geometry geom = status.getGeometry(query);
//        if (geom == null) {
//            return false;
//            // TODO use cache or read from source (how to determine if the cache is complete?)
//        }
//        return status.isDirty(geom);
//    }
//
//    @Override
//    public void setDirty(Query query, boolean value) throws IOException {
//        if (status == null) {
//            throw new IllegalStateException("Status is null");
//        }
//        final Geometry geom = status.getGeometry(query);
//        if (geom == null) {
//            return;
//            // TODO use cache or read from source (how to determine if the cache is complete?)
//        }
//        status.setDirty(geom, value);
//    }
//
//    /**
//     * Override this method to clear the features into the cached feature source <br/>
//     * NOTE: in the overriding method remember to call super.clear().
//     */
//    @Override
//    public void clear() throws IOException {
//        if (status == null) {
//            throw new IllegalStateException("Status is null");
//        }
//        status.clear();
//
//        // if on this instance has been set the entry we may have written some features, let's remove them
//        if (status.getEntry() != null) {
//            FeatureWriter<SimpleFeatureType, SimpleFeature> fw = null;
//            try {
//                fw = cacheManager.getCache().getFeatureWriter(status.getEntry().getTypeName(),
//                        Transaction.AUTO_COMMIT);
//                while (fw.hasNext()) {
//                    fw.next();
//                    fw.remove();
//                }
//            } catch (IOException e) {
//                LOGGER.log(Level.SEVERE, e.getLocalizedMessage(), e);
//            } finally {
//                if (fw != null) {
//                    try {
//                        fw.close();
//                    } catch (IOException e) {
//                        LOGGER.log(Level.WARNING, e.getLocalizedMessage(), e);
//                    }
//                }
//            }
//        }
//        super.clear();
//    }

    // protected static <T> void ehCachePut(Cache ehCacheManager, T value, Object... keys)
    // throws IOException {
    // verify(ehCacheManager);
    // verify(value);
    // verify(keys);
    //
    // if (value != null) {
    // ehCacheManager.put(Arrays.deepHashCode(keys), value);
    // } else {
    // throw new IOException(
    // "Unable to cache a null Object, please check the source datastore.");
    // }
    // }
    //
    // protected static <T> T ehCacheGet(Cache cacheManager, Object... keys) {
    // verify(cacheManager);
    // verify(keys);
    // final SimpleValueWrapper vw = (SimpleValueWrapper) cacheManager.get(Arrays
    // .deepHashCode(keys));
    // if (vw != null) {
    // return (T) vw.get();
    // } else {
    // return null;
    // }
    // }

}
