package org.geotools.data.cache.utils;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.geotools.data.cache.datastore.CacheManager;
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
public class SimpleFeatureCollectionReader extends DelegateSimpleFeature {

    private final SimpleFeatureCollection[] collection;

    private final SimpleFeatureType schema;

    private SimpleFeatureIterator it;

    private int currentCollection = 0;

    public SimpleFeatureCollectionReader(CacheManager cacheManager, SimpleFeatureType schema,
            SimpleFeatureCollection... coll) throws IOException {

        super(cacheManager);

        if (coll == null || coll.length == 0) {
            throw new IllegalArgumentException("Unable to create a " + this.getClass()
                    + " with a null or empty list fo FeatureCollection");
        }

        this.collection = coll;
        final SimpleFeatureCollection c = nextCollection();
        this.it = c.features();
        this.schema = schema;

    }

    @Override
    protected Name getFeatureTypeName() {
        return schema.getName();
    }

    @Override
    protected SimpleFeature getNextInternal() throws IllegalArgumentException,
            NoSuchElementException, IOException {
        return it.next();
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
        if (it != null) {
            try {
                it.close();
            } catch (Exception e) {
            }
        }

    }

}
