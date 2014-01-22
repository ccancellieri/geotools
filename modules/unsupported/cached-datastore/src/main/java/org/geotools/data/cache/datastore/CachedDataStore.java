package org.geotools.data.cache.datastore;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;

import org.geotools.data.DataStore;
import org.geotools.data.Transaction;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.cache.op.SchemaOp;
import org.geotools.data.cache.op.TypeNamesOp;
import org.geotools.data.cache.utils.DelegateContentFeatureSource;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

public class CachedDataStore extends ContentDataStore {

    private final transient CacheManager cacheManager;

    public CachedDataStore(final CacheManager cacheManager) throws IOException {
        if (cacheManager == null)
            throw new IllegalArgumentException(
                    "Unable to initialize the store with a null cache manager");

        this.cacheManager = cacheManager;
    }

    @Override
    protected List<Name> createTypeNames() throws IOException {
        TypeNamesOp typeNamesOp = cacheManager.getCachedOpOfType(Operation.typeNames,
                TypeNamesOp.class);
        List<Name> names = null;
        if (typeNamesOp != null) {

            if (!typeNamesOp.isCached(typeNamesOp.getUid())
                    || typeNamesOp.isDirty(typeNamesOp.getUid())) {
                names = typeNamesOp.updateCache(typeNamesOp.getUid());
                typeNamesOp.setCached(typeNamesOp.getUid(), names != null ? true : false);
            } else {
                names = typeNamesOp.getCache(typeNamesOp.getUid());
            }

        }
        if (names != null) {
            return names;
        } else {
            // the schemas are mandatory for most of the other operations so we have to create them on the cache datastore anyway
            final DataStore source = cacheManager.getSource();
            names = source.getNames();
//            final SchemaOp schemaOp = cacheManager.getCachedOpOfType(Operation.schema,
//                    SchemaOp.class);
//            if (schemaOp != null) {
//                // create schemas
//                for (Name name : names) {
//                    SimpleFeatureType schema = null;
//                    if (!schemaOp.isCached(name) || schemaOp.isDirty(name)) {
//                        schema = schemaOp.updateCache(name);
//                        schemaOp.setCached(name, schema != null ? true : false);
//                    } else {
//                        schema = schemaOp.getCache(name);
//                    }
//                }
//            } else {
//                // create schemas
//                for (Name name : names) {
//                    SimpleFeatureType type = source.getSchema(name);
//                    cacheManager.getCache().createSchema(type);
//                }
//            }
            return names;
        }
    }

    @Override
    protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
        return new DelegateContentFeatureSource(cacheManager, entry, null);
    }

    /**
     * Returns the feature source matching the specified name and explicitly specifies a transaction.
     * <p>
     * Subclasses should not implement this method. However overriding in order to perform a type narrowing to a subclasses of
     * {@link ContentFeatureSource} is acceptable.
     * </p>
     * 
     * @see DataStore#getFeatureSource(String)
     */
    @Override
    public ContentFeatureSource getFeatureSource(Name typeName, Transaction tx) throws IOException {

        final ContentEntry entry = ensureEntry(typeName);
        final ContentFeatureSource featureSource = new DelegateContentFeatureSource(cacheManager,
                entry, null);
        featureSource.setTransaction(tx);

        return featureSource;
    }

    @Override
    public void dispose() {
        super.dispose();
        try {
            cacheManager.dispose();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, e.getMessage(), e);
        }
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }
}
