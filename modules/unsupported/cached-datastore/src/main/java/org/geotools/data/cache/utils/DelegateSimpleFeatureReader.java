package org.geotools.data.cache.utils;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

import org.geotools.data.FeatureReader;
import org.geotools.data.cache.datastore.CacheManager;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

/**
 * 
 * @author carlo cancellieri - GeoSolutions SAS
 * 
 */
public class DelegateSimpleFeatureReader extends DelegateSimpleFeature {

    protected final transient Logger LOGGER = org.geotools.util.logging.Logging
            .getLogger(getClass().getPackage().getName());

    protected FeatureReader<SimpleFeatureType, SimpleFeature> delegate;

    protected final SimpleFeatureType schema;

    public DelegateSimpleFeatureReader(CacheManager cacheManager,
            FeatureReader<SimpleFeatureType, SimpleFeature> delegate, SimpleFeatureType schema)
            throws IOException {

        this(cacheManager, schema);

        setDelegate(delegate);
    }

    /**
     * Be sure to set the delegate member!
     * 
     * @param cacheManager
     * @param schemaName
     * @throws IOException
     */
    public DelegateSimpleFeatureReader(CacheManager cacheManager, SimpleFeatureType schema)
            throws IOException {

        super(cacheManager);

        this.schema = schema;
    }

    protected void setDelegate(FeatureReader<SimpleFeatureType, SimpleFeature> fr) {
        this.delegate = fr;
    }

    @Override
    protected SimpleFeature getNextInternal() throws IllegalArgumentException,
            NoSuchElementException, IOException {
        if (delegate == null)
            throw new IOException("you may set the delegate member");
        return delegate.next();
    }

    @Override
    public boolean hasNext() throws IOException {
        if (delegate == null)
            throw new IOException("you may set the delegate member");
        // TODO hasNextOp
        return delegate.hasNext();
    }

    @Override
    public void close() throws IOException {
        // TODO closeOp
        if (delegate != null) {
            delegate.close();
        } else {
            throw new IOException("you may set the delegate member");
        }
    }

    @Override
    protected Name getFeatureTypeName() {
        return schema.getName();
    }

}
