package org.geotools.data.cache.utils;

import java.io.IOException;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

@SuppressWarnings("unchecked")
public class DelegateContentFeatureSource extends ContentFeatureSource {

    // source
    protected final SimpleFeatureSource delegate;


    public DelegateContentFeatureSource(ContentEntry entry, Query query,
            SimpleFeatureSource delegate) throws IllegalArgumentException {
        super(entry, query);
        if (delegate == null)
            throw new IllegalArgumentException(
                    "Unable to initialize the source cache !=null is needed");

        this.delegate = delegate;
    }

    public SimpleFeatureSource getDelegate() {
        return delegate;
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        return delegate.getBounds(query);
    }

    @Override
    protected int getCountInternal(Query query) throws IOException {
        return delegate.getCount(query);
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
            throws IOException {
        return new SimpleFeatureCollectionReader(delegate.getFeatures(query));
    }

    @Override
    protected SimpleFeatureType buildFeatureType() throws IOException {
        return delegate.getSchema();
    }
}
