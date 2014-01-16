package org.geotools.data.cache.utils;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.FeatureReader;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.cache.op.NextOp;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.cache.op.SchemaOp;
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

    private final transient Logger LOGGER = org.geotools.util.logging.Logging.getLogger(getClass()
            .getPackage().getName());

    private final SimpleFeatureCollection[] collection;

    private final SimpleFeatureType schema;

    // private final CacheManager cacheManager;

    private final SchemaOp schemaOp;

    private final NextOp nextOp;

    private SimpleFeatureIterator it;

    private int currentCollection = 0;

    private final CacheManager cacheManager;

    public SimpleFeatureCollectionReader(CacheManager cacheManager, SimpleFeatureCollection... coll)
            throws IOException {
        if (coll == null || coll.length == 0) {
            throw new IllegalArgumentException("Unable to create a " + this.getClass()
                    + " with a null or empty list fo FeatureCollection");
        } else if (cacheManager == null) {
            throw new IllegalArgumentException("Unable to create a " + this.getClass()
                    + " with a null cacheManager");
        }

        this.collection = coll;
        // this.cacheManager = cacheManager;
        final SimpleFeatureCollection c = nextCollection();
        this.schema = c.getSchema();
        this.it = c.features();

        this.schemaOp = cacheManager.getCachedOpOfType(Operation.schema, SchemaOp.class);
        this.nextOp = cacheManager.getCachedOpOfType(Operation.next, NextOp.class);
        this.cacheManager=cacheManager;
    }

    private SimpleFeatureCollection nextCollection() throws IOException {
        SimpleFeatureCollection coll = collection[this.currentCollection++];
        if (schema != null && !schema.equals(coll.getSchema())) {
            throw new IOException("Unable to read from collections with different schemas");
        }
        return coll;
    }

    private boolean hasNextCollection() {
        return this.currentCollection < collection.length;
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        SimpleFeatureType cachedSchema = null;
        if (schemaOp != null) {
            final Name name = schema.getName();
            try {
                if (!schemaOp.isCached(name) || schemaOp.isDirty(name)) {
                    cachedSchema = schemaOp.updateCache(name);
                    schemaOp.setCached(name, schema != null ? true : false);
                } else {
                    cachedSchema = schemaOp.getCache(name);
                }
            } catch (IOException e) {
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.log(Level.SEVERE, e.getMessage(), e);
                }
            }
        }
        if (cachedSchema != null) {
            return cachedSchema;
        } else {
            return schema;
        }
    }

    @Override
    public SimpleFeature next() throws IOException, IllegalArgumentException,
            NoSuchElementException {
        final SimpleFeature sf = it.next();
        final SimpleFeature df = sf;
        // try using next operation
        if (nextOp != null) {
            SimpleFeature feature = null;
            nextOp.setSf(sf);
            nextOp.setDf(df);
            if (!nextOp.isCached(sf.getIdentifier()) || nextOp.isDirty(sf.getIdentifier())) {
                feature = nextOp.updateCache(feature.getIdentifier());
                nextOp.setCached(sf.getIdentifier(), feature != null ? true : false);
            } else {
                feature = nextOp.getCache(sf.getIdentifier());
            }
            if (feature != null) {
                return feature;
            }
        }
        return df;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (it.hasNext()) {
            return true;
        } else if (hasNextCollection()) {
            it.close();
            it = nextCollection().features();
            return hasNext();
        } else {
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        it.close();
    }

}
