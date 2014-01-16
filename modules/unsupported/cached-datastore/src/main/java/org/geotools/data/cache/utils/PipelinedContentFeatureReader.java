package org.geotools.data.cache.utils;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.cache.op.NextOp;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.cache.op.SchemaOp;
import org.geotools.data.store.ContentEntry;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

public class PipelinedContentFeatureReader implements
        FeatureReader<SimpleFeatureType, SimpleFeature> {
    private final transient Logger LOGGER = org.geotools.util.logging.Logging.getLogger(getClass()
            .getPackage().getName());

    private final CacheManager cacheManager;

    private final NextOp nextOp;

    private final SchemaOp schemaOp;

    private final ContentEntry entry;

    private FeatureWriter<SimpleFeatureType, SimpleFeature> fw = null;

    private FeatureReader<SimpleFeatureType, SimpleFeature> fr = null;

    public PipelinedContentFeatureReader(ContentEntry entry, final Query origQuery, final Query integratedQuery,
            final CacheManager cacheManager) throws IOException {
        this.entry = entry;
        this.cacheManager = cacheManager;
        this.nextOp = cacheManager.getCachedOpOfType(Operation.next, NextOp.class);
        this.schemaOp = cacheManager.getCachedOpOfType(Operation.schema, SchemaOp.class);

        fr = cacheManager.getSource().getFeatureReader(integratedQuery, Transaction.AUTO_COMMIT);
        fw = cacheManager.getCache().getFeatureWriterAppend(integratedQuery.getTypeName(),
                Transaction.AUTO_COMMIT);
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        try {
            final SimpleFeatureType schema = this.cacheManager.getSource().getSchema(
                    entry.getTypeName());
            SimpleFeatureType cachedSchema = null;
            if (schemaOp != null) {
                final Name name = schema.getName();
                if (!schemaOp.isCached(cacheManager.getUID())) {
                    cachedSchema = schemaOp.updateCache(name);
                    schemaOp.setCached(cachedSchema != null ? true : false, cacheManager.getUID());
                } else {
                    cachedSchema = schemaOp.getCache(name);
                }
            }
            if (cachedSchema != null) {
                return cachedSchema;
            } else {
                return schema;
            }
        } catch (IOException e) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.log(Level.SEVERE, e.getLocalizedMessage(), e);
            }
        }
        return null;
    }

    @Override
    public SimpleFeature next() throws IOException, IllegalArgumentException,
            NoSuchElementException {
        final SimpleFeature sf = fr.next();
        final SimpleFeature df = fw.next();
        // try using next operation
        if (nextOp != null) {
            SimpleFeature feature = null;
            nextOp.setSf(sf);
            nextOp.setDf(df);
            if (!nextOp.isCached(sf.getIdentifier())) {
                feature = nextOp.updateCache(sf.getIdentifier());
                nextOp.setCached(feature != null ? true : false, cacheManager.getUID());
            } else {
                feature = nextOp.getCache(sf.getIdentifier());
            }
            if (feature != null) {
                fw.write();
                return feature;
            }
        }
        for (Property p : sf.getProperties()) {
            df.getProperty(p.getName()).setValue(p.getValue());
        }
        fw.write();
        
        // TODO filter over origQuery
        
        return df;
    }

    @Override
    public boolean hasNext() throws IOException {
        return fr.hasNext();
    }

    @Override
    public void close() throws IOException {
        if (fr != null) {
            try {
                fr.close();
            } catch (IOException e) {
            }
        }
        if (fw != null) {
            try {
                fw.close();
            } catch (IOException e) {
            }
        }
    }

}
