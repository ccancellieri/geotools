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

public class SimpleFeatureUpdaterReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {
    private final transient Logger LOGGER = org.geotools.util.logging.Logging.getLogger(getClass()
            .getPackage().getName());

    private final CacheManager cacheManager;

    private final NextOp nextOp;

    private final SchemaOp schemaOp;

    private final ContentEntry entry;

    private final Transaction transaction;

    private FeatureWriter<SimpleFeatureType, SimpleFeature> fw = null;

    public SimpleFeatureUpdaterReader(ContentEntry entry, final Query query,
            final CacheManager cacheManager) throws IOException {
        this(entry, query, cacheManager, Transaction.AUTO_COMMIT);
    }

    public SimpleFeatureUpdaterReader(ContentEntry entry, final Query query,
            final CacheManager cacheManager, final Transaction transaction) throws IOException {

        this.entry = entry;
        this.cacheManager = cacheManager;
        this.nextOp = cacheManager.getCachedOpOfType(Operation.next, NextOp.class);
        this.schemaOp = cacheManager.getCachedOpOfType(Operation.schema, SchemaOp.class);
        this.transaction = transaction;

        fw = cacheManager.getCache().getFeatureWriter(query.getTypeName(), transaction);

    }

    @Override
    public SimpleFeatureType getFeatureType() {
        try {
            final SimpleFeatureType schema = this.cacheManager.getSource().getSchema(
                    entry.getTypeName());
            SimpleFeatureType cachedSchema = null;
            if (schemaOp != null) {
                final Name name = schema.getName();
                if (!schemaOp.isCached(name)) {
                    cachedSchema = schemaOp.updateCache(name);
                    schemaOp.setCached(name, cachedSchema != null ? true : false);
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
        final SimpleFeature df = fw.next();
        // try using next operation
        if (nextOp != null) {
            SimpleFeature feature = null;
            nextOp.setSf(df);
            nextOp.setDf(df);
            if (!nextOp.isCached(df.getIdentifier()) || nextOp.isDirty(df.getIdentifier())) {
                feature = nextOp.updateCache(df.getIdentifier());
                nextOp.setCached(df.getIdentifier(), feature != null ? true : false);
            } else {
                feature = nextOp.getCache(df.getIdentifier());
            }
            if (feature != null) {
                fw.write();
                return feature;
            }
        }
        // for (Property p : sf.getProperties()) {
        // df.getProperty(p.getName()).setValue(p.getValue());
        // }
        // fw.write();
        return df;
    }

    @Override
    public boolean hasNext() throws IOException {
        return fw.hasNext();
    }

    @Override
    public void close() throws IOException {
        if (fw != null) {
            try {
                fw.close();
            } catch (IOException e) {
            }
        }
    }

}
