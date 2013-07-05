package org.geotools.data.cache.utils;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.cache.op.CachedOp;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

/**
 * Use CacheManager to try cached operations if no operation is defined then delegates to the passed {@link SimpleFeatureSource}
 * 
 * @author carlo cancellieri - GeoSolutions SAS
 * 
 */
@SuppressWarnings("unchecked")
public class DelegateContentFeatureSource extends ContentFeatureSource {
    private final transient Logger LOGGER = org.geotools.util.logging.Logging.getLogger(getClass()
            .getPackage().getName());

    // source
    protected final SimpleFeatureSource delegate;

    protected final CacheManager cacheManager;

    public SimpleFeatureSource getDelegate() {
        return delegate;
    }

    public DelegateContentFeatureSource(final CacheManager cacheManager, final ContentEntry entry,
            final Query query, final SimpleFeatureSource delegate) throws IllegalArgumentException {
        super(entry, query);
        if (delegate == null)
            throw new IllegalArgumentException("Unable to initialize a delegate !=null is needed");
        this.delegate = delegate;
        if (cacheManager == null)
            throw new IllegalArgumentException(
                    "Unable to initialize a cache manager !=null is needed");
        this.cacheManager = cacheManager;
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        final CachedOp<ReferencedEnvelope, Query, Query> op = (CachedOp<ReferencedEnvelope, Query, Query>) cacheManager
                .getCachedOp(Operation.bounds);
        if (op != null) {
            try {
                if (!op.isCached(query)) {
                    op.putCache(delegate.getBounds(query));
                    op.setCached(true, query);
                }
                return op.getCache(query);

            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }
        return delegate.getBounds(query);
    }

    @Override
    protected int getCountInternal(Query query) throws IOException {
        final CachedOp<Integer, Query, Query> op = (CachedOp<Integer, Query, Query>) cacheManager
                .getCachedOp(Operation.count);
        if (op != null) {
            try {
                if (!op.isCached(query)) {
                    op.putCache(delegate.getCount(query));
                    op.setCached(true, query);
                }
                return op.getCache(query);
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }
        return delegate.getCount(query);
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
            throws IOException {
        final CachedOp<SimpleFeatureCollection, Query, Name> op = (CachedOp<SimpleFeatureCollection, Query, Name>) cacheManager
                .getCachedOp(Operation.featureCollection);
        if (op != null) {
            try {
                if (!op.isCached(delegate.getName())) {
                    op.putCache(delegate.getFeatures(query));
                    op.setCached(true, delegate.getName());
                }
                return new SimpleFeatureCollectionReader(cacheManager, op.getCache(query));
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }
        return new SimpleFeatureCollectionReader(cacheManager, delegate.getFeatures(query));
    }

    @Override
    protected SimpleFeatureType buildFeatureType() throws IOException {
        final CachedOp<SimpleFeatureType, Name, Name> op = (CachedOp<SimpleFeatureType, Name, Name>) cacheManager
                .getCachedOp(Operation.schema);
        if (op != null) {
            SimpleFeatureType schema = delegate.getSchema();
            Name name = schema.getName();
            try {
                if (!op.isCached(name)) {
                    op.putCache(schema);
                    op.setCached(true, name);
                }
                return op.getCache(name);
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }
        return delegate.getSchema();
    }

}
