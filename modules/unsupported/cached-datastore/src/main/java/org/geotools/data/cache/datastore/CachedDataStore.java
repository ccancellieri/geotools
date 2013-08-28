package org.geotools.data.cache.datastore;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;

import org.geotools.data.DataStore;
import org.geotools.data.Transaction;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.cache.op.TypeNamesOp;
import org.geotools.data.cache.utils.DelegateContentFeatureSource;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.opengis.feature.type.Name;

public class CachedDataStore extends ContentDataStore {

    // cached datastore
    private final DataStore store;

    private final CacheManager cacheManager;

    public CachedDataStore(final CacheManager cacheManager) throws IOException {
        if (cacheManager == null)
            throw new IllegalArgumentException(
                    "Unable to initialize the store with a null cache manager");

        this.cacheManager = cacheManager;

        this.store = cacheManager.getSource();
    }

    @Override
    protected List<Name> createTypeNames() throws IOException {
        TypeNamesOp cachedOp = cacheManager.getCachedOpOfType(Operation.typeNames,TypeNamesOp.class);
        List<Name> names=null;
        if (cachedOp != null) {
            try {
                if (!cachedOp.isCached(null)) {
                    names=cachedOp.updateCache(null);
                    cachedOp.setCached(names!=null?true:false, null);
                } else {
                    names=cachedOp.getCache(null);
                }
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, e.getMessage(), e);
            }
        }
        if (names!=null){
            return names;
        } else {
            return store.getNames();
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
        cacheManager.dispose();
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }
}
