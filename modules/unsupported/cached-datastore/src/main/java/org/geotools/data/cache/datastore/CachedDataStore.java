package org.geotools.data.cache.datastore;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.DataStore;
import org.geotools.data.Transaction;
import org.geotools.data.cache.op.CachedOpSPI;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.cache.op.feature.BaseFeatureOpStatus;
import org.geotools.data.cache.op.typename.TypeNamesOp;
import org.geotools.data.cache.utils.DelegateContentFeatureSource;
import org.geotools.data.cache.utils.EHCacheUtils;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.opengis.feature.type.Name;

public class CachedDataStore extends ContentDataStore {

    private final transient Logger LOGGER = org.geotools.util.logging.Logging.getLogger(getClass()
            .getPackage().getName());

    private final transient DataStore source;

    private final transient DataStore cache;

//    private final Map<String, CacheStatus> cacheStatusMap = new HashMap<String, CacheStatus>();
    
    private final Map<String, CacheManager> cacheStatusMap = new HashMap<String, CacheManager>();

    private final String uid;

    public CachedDataStore(final DataStore source, final DataStore cache, final String uid,
            final Map<String, CachedOpSPI<?,?,?>> spiParams) throws IOException {

        if (source == null || cache == null || uid == null || spiParams == null)
            throw new IllegalArgumentException("Unable to initialize the store with a null param");

        this.source = source;
        this.cache = cache;
        this.uid = uid;
    }

    @Override
    protected List<Name> createTypeNames() throws IOException {
        TypeNamesOp typeNamesOp = cacheStatusMap.get.getCachedOpOfType(Operation.typeNames,
                TypeNamesOp.class);
        List<Name> names = null;
        if (typeNamesOp != null) {

            if (!typeNamesOp.isCached(uid)
                    || typeNamesOp.isDirty(uid)) {
                names = typeNamesOp.updateCache(uid);
                typeNamesOp.setCached(uid, names != null ? true : false);
            } else {
                names = typeNamesOp.getCache(uid);
            }
        }
        if (names != null) {
            return names;
        } else {
            // the schemas are mandatory for most of the other operations so we have to create them on the cache datastore anyway
            final DataStore source = cacheStatusMap.getSource();
            names = source.getNames();
            // final SchemaOp schemaOp = cacheStatusMap.getCachedOpOfType(Operation.schema,
            // SchemaOp.class);
            // if (schemaOp != null) {
            // // create schemas
            // for (Name name : names) {
            // SimpleFeatureType schema = null;
            // if (!schemaOp.isCached(name) || schemaOp.isDirty(name)) {
            // schema = schemaOp.updateCache(name);
            // schemaOp.setCached(name, schema != null ? true : false);
            // } else {
            // schema = schemaOp.getCache(name);
            // }
            // }
            // } else {
            // // create schemas
            // for (Name name : names) {
            // SimpleFeatureType type = source.getSchema(name);
            // cacheStatusMap.getCache().createSchema(type);
            // }
            // }
            return names;
        }
    }

    @Override
    protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
        final CacheManager cacheManager = getCacheManager(entry.getName().getLocalPart());
        BaseFeatureOpStatus fs = cacheManager.getStatus().getFeatureStatus();
        fs.setEntry(entry);
        return new DelegateContentFeatureSource(cacheManager, null, fs);
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
        final CacheManager cacheManager = getCacheManager(typeName.getLocalPart());
        final BaseFeatureOpStatus fs = cacheManager.getStatus().getFeatureStatus();
        fs.setEntry(ensureEntry(typeName));
        final ContentFeatureSource featureSource = new DelegateContentFeatureSource(cacheManager,
                null, fs);
        featureSource.setTransaction(tx);
        return featureSource;
    }

    @Override
    public void dispose() {
        super.dispose();
        for (String key : cacheStatusMap.keySet()) {
            try {
                cacheStatusMap.get(key).dispose();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "UID:" + key + " " + e.getMessage(), e);
            }
        }
    }

    public CacheManager getCacheManager(String typeName) {
        CacheStatus status = cacheStatusMap.get(typeName);
        if (status == null) {
            throw new IllegalStateException("Unable to create a manager with a null status");
        }
        return new CacheManager(source, cache, status);
    }

    /**
     * loads the cachedOpSPIMap status from the ehcache
     */
    void load() {
        String typeName;
        
        final CacheStatus cacheStatus = EHCacheUtils.load(typeName);
        
        // recursive load
        for (CacheStatus m : cacheStatusMap.values()) {
            // load the set into the cache map
            final CacheStatus cacheStatus = EHCacheUtils.load(m.getUID());
            if (cachedStatus != null) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Loading " + cachedStatus.getUID() + " from cache ("
                            + cachedStatus.getUID().hashCode() + ")");
                }
                this.cacheStatusMap.put(cachedStatus.getUID(), cachedStatus);
            } else {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.log(Level.WARNING, "No entry load from cache for key: " + m.getUID());
                }
            }
        }
    }

    /**
     * Recursively save the current status
     * 
     * @throws IOException
     */
    void save() throws IOException {
        EHCacheUtils.store(cre, cacheStatusMap);
        // recursive save
        for (CacheStatus m : cacheStatusMap.values()) {
            // store the set into the cache
            EHCacheUtils.store(m.getUID(), m);
        }
    }

    /**
     * clear the cache status and all of the sub caches
     */
    void clear() throws IOException {

    }

}
