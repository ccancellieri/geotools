package org.geotools.data.cache.datastore;

import java.io.IOException;
import java.util.List;

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
        TypeNamesOp cachedOp = (TypeNamesOp) cacheManager.getCachedOp(Operation.typeNames);
        if (cachedOp != null) {
            if (cachedOp.isCached()) {
                return (List<Name>) cachedOp.getCache();
            } else {
                final List<Name> names = store.getNames();
                cachedOp.putCache(names);
                return names;
            }
        } else {
            return store.getNames();
        }
    }

    public DataStore getStore() {
        return store;
    }

    @Override
    protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
        return new DelegateContentFeatureSource(cacheManager, entry, null,
                store.getFeatureSource(entry.getTypeName()));
    }
    
    
    /**
     * Returns the feature source matching the specified name and explicitly 
     * specifies a transaction.
     * <p>
     * Subclasses should not implement this method. However overriding in order 
     * to perform a type narrowing to a subclasses of {@link ContentFeatureSource}
     * is acceptable.
     * </p>
     *
     * @see DataStore#getFeatureSource(String)
     */
    @Override
    public ContentFeatureSource getFeatureSource(Name typeName, Transaction tx)
        throws IOException {
        
        ContentEntry entry = ensureEntry(typeName);
        ContentFeatureSource featureSource = new DelegateContentFeatureSource(cacheManager, entry, null,
                store.getFeatureSource(entry.getTypeName()));
        featureSource.setTransaction(tx);
        
        
        return featureSource;
    }
    
    @Override
    public void dispose() {
        super.dispose();
        cacheManager.dispose();
    }
}
