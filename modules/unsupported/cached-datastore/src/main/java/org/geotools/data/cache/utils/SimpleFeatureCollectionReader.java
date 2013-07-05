package org.geotools.data.cache.utils;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.geotools.data.FeatureReader;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.cache.op.CachedOp;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

/**
 * 
 * @author carlo cancellieri - GeoSolutions SAS
 * 
 */
public class SimpleFeatureCollectionReader implements
        FeatureReader<SimpleFeatureType, SimpleFeature> {

    private final SimpleFeatureIterator it;

    private final SimpleFeatureCollection collection;

    private final CacheManager cacheManager;

    public SimpleFeatureCollectionReader(CacheManager cacheManager, SimpleFeatureCollection coll) {
        this.collection = coll;
        this.it = coll.features();
        this.cacheManager = cacheManager;
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        final CachedOp<SimpleFeatureType, Name, Name> op = (CachedOp<SimpleFeatureType, Name, Name>) cacheManager
                .getCachedOp(Operation.schema);
        if (op != null) {
            SimpleFeatureType schema = collection.getSchema();
            Name name = schema.getName();
            try {
                if (!op.isCached(name)) {
                    op.putCache(schema);
                    op.setCached(true, name);
                }
                return op.getCache(name);
            } catch (IOException e) {
                // LOGGER.log(Level.FINER, e.getMessage(), e);
            }
        }
        return collection.getSchema();
    }

    @Override
    public SimpleFeature next() throws IOException, IllegalArgumentException,
            NoSuchElementException {
        return it.next();
    }

    @Override
    public boolean hasNext() throws IOException {
        return it.hasNext();
    }

    @Override
    public void close() throws IOException {
        it.close();
    }

}
