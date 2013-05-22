package org.geotools.data.cache.utils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.geotools.data.FeatureReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class SimpleListReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    private final Iterator<SimpleFeature> it;

    private final List<SimpleFeature> collection;

    private final SimpleFeatureType schema;

    public SimpleListReader(List<SimpleFeature> coll, SimpleFeatureType schema) {
        this.collection = coll;
        this.it = coll.iterator();
        this.schema = schema;
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return schema;
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
        // do nothing
    }

}
