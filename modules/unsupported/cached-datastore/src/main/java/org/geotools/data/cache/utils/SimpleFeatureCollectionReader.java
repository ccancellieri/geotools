package org.geotools.data.cache.utils;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.geotools.data.FeatureReader;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * 
 * @author carlo cancellieri - GeoSolutions SAS
 *
 */
public class SimpleFeatureCollectionReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    private final SimpleFeatureIterator it;

    private final SimpleFeatureCollection collection;

    public SimpleFeatureCollectionReader(SimpleFeatureCollection coll) {
        this.collection = coll;
        this.it = coll.features();
    }

    @Override
    public SimpleFeatureType getFeatureType() {
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
