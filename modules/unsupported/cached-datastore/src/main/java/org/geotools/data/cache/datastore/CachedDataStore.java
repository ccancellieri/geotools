package org.geotools.data.cache.datastore;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.DataStore;
import org.geotools.data.Transaction;
import org.geotools.data.cache.op.CachedOp;
import org.geotools.data.cache.op.CachedOpSPI;
import org.geotools.data.cache.op.CachedOpStatus;
import org.geotools.data.cache.op.Operation;
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

    private final Map<String, CacheManager> cacheManagerMap = new HashMap<String, CacheManager>();

    // private Map<String, CacheStatus> cacheStatusMap;

    final Map<String, CachedOpSPI<CachedOpStatus<?>, CachedOp<?, ?>, ?, ?>> spiParams;

    private final String uid;

    private TypeNamesOp typeNamesOp;

    public CachedDataStore(final DataStore source, final DataStore cache, final String uid,
            final Map<String, CachedOpSPI<CachedOpStatus<?>, CachedOp<?, ?>, ?, ?>> spiParams)
            throws IOException {

        if (source == null || cache == null || uid == null || spiParams == null)
            throw new IllegalArgumentException("Unable to initialize the store with a null param");

        this.source = source;
        this.cache = cache;
        this.uid = uid;
        this.spiParams = spiParams;

        // todo: check changes between passed params and loaded cachedStatusMap

        final CachedOpSPI<CachedOpStatus<?>, CachedOp<?, ?>, ?, ?> typeNamesSPI = this.spiParams
                .remove(Operation.typeNames.toString());
//        final String typeNameUid = createTypeNamesOpUID();

        // if ((cacheStatusMap = EHCacheUtils.load(uid)) == null) {
        // cacheStatusMap = new HashMap<String, CacheStatus>();

        // building/loading typeNames op
        if (typeNamesSPI != null) {
            final String name = Operation.typeNames.toString();
            // create an SPI map with only one operation (for TypeNames)
            final Map<String, CachedOpSPI<CachedOpStatus<?>, CachedOp<?, ?>, ?, ?>> typeNameOpSPIMap = new HashMap<String, CachedOpSPI<CachedOpStatus<?>, CachedOp<?, ?>, ?, ?>>();
            typeNameOpSPIMap.put(name, typeNamesSPI);
            // create the manager
            final CacheManager cm = new CacheManager(source, cache, createCacheStatusUID(name), typeNameOpSPIMap, false);
            // store created manager
            cacheManagerMap.put(name, cm);
            // initialize typeNamesOp
            typeNamesOp = (TypeNamesOp) cm.getCachedOp(Operation.typeNames);
        } else {
            typeNamesOp = null;
        }

        for (Name typeName : createTypeNames()) {
            final String name = typeName.getLocalPart();
//            final CacheStatus cs = new CacheStatus(name, spiParams, true);
//            cacheStatusMap.put(name, cs);
//            final CacheManager cm = 
            cacheManagerMap.put(name, new CacheManager(source, cache, createCacheStatusUID(name), spiParams ,true));
        }
        // } else {
        // if (typeNamesSPI != null) {
        // final CacheStatus cs = cacheStatusMap.get(typeNameUid);
        // if (cs == null) {
        // throw new IllegalStateException("Unable to locate the cacheManager for: "
        // + typeNameUid);
        // }
        // final CacheManager cm = new CacheManager(source, cache, cs);
        // cacheManagerMap.put(typeNameUid, cm);
        //
        // typeNamesOp = (TypeNamesOp) cm.getCachedOp(Operation.typeNames);
        // } else {
        // typeNamesOp = null;
        // }
        //
        // for (Name typeName : createTypeNames()) {
        // final String name = typeName.getLocalPart();
        // final CacheStatus cs = cacheStatusMap.get(name);
        // if (cs == null) {
        // throw new IllegalStateException("Unable to locate the cacheManager for: "
        // + typeNameUid);
        // }
        // final CacheManager cm = new CacheManager(source, cache, cs);
        // cacheManagerMap.put(name, cm);
        // }
        // }
    }

    @Override
    public void dispose() {
        super.dispose();

//        if (typeNamesOp != null) {
//            try {
//                typeNamesOp.dispose();
//            } catch (IOException e) {
//                LOGGER.log(Level.WARNING,
//                        "Status:" + typeNamesOp.getStatus() + " " + e.getMessage(), e);
//            }
//        }

        for (String mngrkey : cacheManagerMap.keySet()) {
            try {
                cacheManagerMap.get(mngrkey).dispose();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "UID:" + mngrkey + " " + e.getMessage(), e);
            }
        }

        // dispose source
        if (source != null) {
            source.dispose();
        }

        // dispose cache
        if (cache != null) {
            cache.dispose();
        }

        // save
        try {
            save();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, e.getMessage(), e);
        }
    }

    /**
     * Recursively save the current status
     * 
     * @throws IOException
     */
    void save() throws IOException {
        if (cacheManagerMap != null) {
            for (CacheManager cs : cacheManagerMap.values()) {
                if (cs != null) {
                    cs.save();
                }
            }
        }
        EHCacheUtils.flush();
    }

    /**
     * clear the cache status and all of the sub caches
     */
    public void clear() throws IOException {
        if (cacheManagerMap != null) {
            for (CacheManager cs : cacheManagerMap.values()) {
                if (cs != null) {
                    cs.clear();
                }
            }
        }
        EHCacheUtils.flush();
    }

    @Override
    protected List<Name> createTypeNames() throws IOException {

        List<Name> names = null;
        if (typeNamesOp != null) {

            if (!typeNamesOp.isCached(uid) || typeNamesOp.isDirty(uid)) {
                names = typeNamesOp.updateCache(uid);
                typeNamesOp.save();
            } else {
                names = typeNamesOp.getCache(uid);
            }
        }
        if (names != null) {
            return names;
        } else {
            // the schemas are mandatory for most of the other operations so we have to create them on the cache datastore anyway
            return source.getNames();
        }
    }

    @Override
    protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
        return buildFeatureSource(entry);
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
        final ContentFeatureSource featureSource = buildFeatureSource(ensureEntry(typeName));
        featureSource.setTransaction(tx);
        return featureSource;
    }

    private ContentFeatureSource buildFeatureSource(ContentEntry contentEntry)
            throws IllegalArgumentException, IOException {
        final String typeName = contentEntry.getName().getLocalPart();
        final CacheManager cacheManager = cacheManagerMap.get(typeName);
        return new DelegateContentFeatureSource(cacheManager, null, contentEntry);
    }

    // public CacheManager getCacheManager(String typeName) {
    // CacheStatus status = cacheStatusMap.get(typeName);
    // if (status == null) {
    // throw new IllegalStateException("Unable to create a manager with a null status");
    // }
    // return new CacheManager(source, cache, status);
    // }

    private String createCacheStatusUID(String typeName) {
        return new StringBuilder(uid).append(':').append(typeName).toString();
    }

    // /**
    // * set the new spi collection comparing the stored set of SPI with the passed one and returning a collection of operation which are changed
    // (this
    // * should be used to clear the matching cachedOp before a new one is created into the CacheManager)
    // *
    // * @param spiColl
    // * @return
    // */
    // Collection<Operation> setCachedOpSPI(Collection<CachedOpSPI<CachedOpStatus<?>, ?, ?, ?>> spiColl) {
    // final boolean sharedFeatureStatus = true;
    // BaseFeatureOpStatus featureStatus = null;
    // boolean clearCheckLoop = false;
    //
    // final List<Operation> returns = new ArrayList<Operation>();
    // try {
    // this.statusLock.writeLock().lock();
    //
    // // for each selected SPI changes
    // final Iterator<CachedOpSPI<CachedOpStatus<?>, ?, ?, ?>> it = spiColl.iterator();
    // while (it.hasNext()) {
    // final CachedOpSPI<CachedOpStatus<?>, ?, ?, ?> selectedOpSPI = it.next();
    // // check stored to compare with selected SPI
    // if (cachedOpSPIMap.containsKey(selectedOpSPI.getOp())) {
    // final CachedOpSPI<?, ?, ?, ?> storedOpSPI = cachedOpSPIMap.get(selectedOpSPI
    // .getOp().toString());
    // if (selectedOpSPI.getOp().equals(storedOpSPI.getOp())) {
    // // if stored is equals with the selected no change is required
    // if (!selectedOpSPI.equals(storedOpSPI)) {
    // // substitute the the stored operation with the new one
    // cachedOpSPIMap.put(selectedOpSPI.getOp().toString(), selectedOpSPI);
    // if (sharedFeatureStatus && featureStatus != null
    // && featureStatus.isApplicable(selectedOpSPI.getOp())) {
    // cachedOpStatusMap.put(selectedOpSPI.getOp().toString(),
    // featureStatus);
    // } else {
    // CachedOpStatus<?> status = selectedOpSPI.createStatus();
    // if (status != null
    // && BaseFeatureOpStatus.class.isAssignableFrom(status
    // .getClass())) {
    // featureStatus = (BaseFeatureOpStatus) status;
    // cachedOpStatusMap.put(selectedOpSPI.getOp().toString(),
    // featureStatus);
    // } else {
    // cachedOpStatusMap.put(selectedOpSPI.getOp().toString(), status);
    // }
    // }
    //
    // // add the operation to the return list
    // returns.add(storedOpSPI.getOp());
    // }
    // }
    // } else {
    // // the selected SPI is not present into the store map, let's add it
    // cachedOpSPIMap.put(selectedOpSPI.getOp().toString(), selectedOpSPI);
    // cachedOpStatusMap.put(selectedOpSPI.getOp().toString(),
    // selectedOpSPI.createStatus());
    // }
    // }
    //
    // // now some SPI into the cachedOpSPIMap may be absent into the selection (unselected)
    // // lets remove them from the stored map adding them to the returns list.
    // final Iterator<Entry<String, CachedOpSPI<CachedOpStatus<?>, ?, ?, ?>>> itSpi = cachedOpSPIMap
    // .entrySet().iterator();
    // final Iterator<Entry<String, CachedOpStatus<?>>> itStatus = cachedOpStatusMap
    // .entrySet().iterator();
    // while (itSpi.hasNext()) {
    // final Entry<String, CachedOpSPI<CachedOpStatus<?>, ?, ?, ?>> entry = itSpi.next();
    // final Entry<String, CachedOpStatus<?>> status = itStatus.next();
    //
    // final String op = entry.getKey();
    // // if the selected operationSPI collection does not contains some SPI in the stored map
    // // boolean found = false;
    // final Iterator<CachedOpSPI<CachedOpStatus<?>, ?, ?, ?>> it4 = spiColl.iterator();
    // while (it4.hasNext()) {
    // final CachedOpSPI<?, ?, ?, ?> selectedOpSPI = it4.next();
    // if (selectedOpSPI.getOp().toString().equals(op)) {
    // // add to the change list
    // returns.add(Operation.valueOf(op));
    // try {
    // if (!sharedFeatureStatus) {
    // status.getValue().clear();
    // } else if (featureStatus != null && status.equals(featureStatus)) {
    // // see below for cache memory leak clear loop
    // clearCheckLoop = true;
    // }
    // } catch (IOException e) {
    // LOGGER.severe(e.getMessage());
    // }
    // // remove from the store
    // itSpi.remove();
    // itStatus.remove();
    // break;
    // }
    // }
    // }
    // } finally {
    // this.statusLock.writeLock().unlock();
    // }
    //
    // // cache memory leak clear loop
    // if (clearCheckLoop) {
    // boolean found = false;
    // final Iterator<Entry<String, CachedOpStatus<?>>> itStatus = cachedOpStatusMap
    // .entrySet().iterator();
    // while (itStatus.hasNext()) {
    // final Entry<String, CachedOpStatus<?>> status = itStatus.next();
    // if (status.equals(featureStatus)) {
    // found = true;
    // }
    // }
    // if (!found) {
    // try {
    // featureStatus.clear();
    // } catch (IOException e) {
    // LOGGER.severe(e.getMessage());
    // }
    // }
    // }
    // return returns;
    // }

    //
    // /**
    // * loads the cachedOpSPIMap status from the ehcache
    // */
    // void load() {
    //
    // // recursive load
    // for (CacheStatus status : cacheStatusMap.values()) {
    // // load the set into the cache map
    // final CacheStatus status = EHCacheUtils.load(status.getUID());
    // if (cachedStatus != null) {
    // if (LOGGER.isLoggable(Level.INFO)) {
    // LOGGER.info("Loading " + cachedStatus.getUID() + " from cache ("
    // + cachedStatus.getUID().hashCode() + ")");
    // }
    // this.cacheStatusMap.put(cachedStatus.getUID(), cachedStatus);
    // } else {
    // if (LOGGER.isLoggable(Level.WARNING)) {
    // LOGGER.log(Level.WARNING, "No entry load from cache for key: " + m.getUID());
    // }
    // }
    // }
    // }

}
