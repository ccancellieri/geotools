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
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

/**
 * 
 * @author carlo cancellieri - GeoSolutions SAS
 * 
 */
public class PipelinedSimpleFeatureReader implements
        FeatureReader<SimpleFeatureType, SimpleFeature> {

    private final transient Logger LOGGER = org.geotools.util.logging.Logging.getLogger(getClass()
            .getPackage().getName());

    private final FeatureReader<SimpleFeatureType, SimpleFeature>[] frc;

    private final SimpleFeatureType schema;

//    private final CacheManager cacheManager;
    
    private final SchemaOp schemaOp;
    
    private final NextOp nextOp; 

    private FeatureReader<SimpleFeatureType, SimpleFeature> fr;

    private int currentReader = 0;

    public PipelinedSimpleFeatureReader(CacheManager cacheManager,
            FeatureReader<SimpleFeatureType, SimpleFeature>... coll) throws IOException {
        if (coll == null || coll.length == 0) {
            throw new IllegalArgumentException("Unable to create a " + this.getClass()
                    + " with a null or empty list fo FeatureCollection");
        } else if (cacheManager == null) {
            throw new IllegalArgumentException("Unable to create a " + this.getClass()
                    + " with a null cacheManager");
        }

        this.frc = coll;
//        this.cacheManager = cacheManager;
        this.fr = nextReader();
        this.schema = fr.getFeatureType();

        this.schemaOp = cacheManager.getCachedOpOfType(Operation.schema, SchemaOp.class);
        this.nextOp = cacheManager.getCachedOpOfType(Operation.next, NextOp.class);
    }

    private FeatureReader<SimpleFeatureType, SimpleFeature> nextReader() throws IOException {
        FeatureReader<SimpleFeatureType, SimpleFeature> fr = frc[this.currentReader++];
        if (schema != null && schema.equals(fr.getFeatureType())) {
            throw new IOException("Unable to read from collections with different schemas");
        }
        return fr;
    }

    private boolean hasNextCollection() {
        return this.currentReader < frc.length;
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        SimpleFeatureType cachedSchema = null;
        if (schemaOp != null) {
            final Name name = schema.getName();
            try {
                if (!schemaOp.isCached(name)) {
                    cachedSchema = schemaOp.updateCache(name);
                    schemaOp.setCached(schema != null ? true : false, name);
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
        final SimpleFeature sf = fr.next();
        final SimpleFeature df = sf;
        // try using next operation
        if (nextOp != null) {
            SimpleFeature feature = null;
            nextOp.setSf(sf);
            nextOp.setDf(df);
            if (!nextOp.isCached(null)) {
                feature = nextOp.updateCache(null);
                nextOp.setCached(feature != null ? true : false, feature.getID());
            } else {
                feature = nextOp.getCache(null);
            }
            if (feature != null) {
                return feature;
            }
        }
        for (Property p : sf.getProperties()) {
            df.getProperty(p.getName()).setValue(p.getValue());
        }
        return df;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (fr.hasNext()) {
            return true;
        } else if (hasNextCollection()) {
            fr.close();
            fr = nextReader();
            return hasNext();
        } else {
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        fr.close();
    }

}
