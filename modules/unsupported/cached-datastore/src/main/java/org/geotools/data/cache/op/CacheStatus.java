package org.geotools.data.cache.op;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.cache.utils.EHCacheUtils;
import org.springframework.cache.Cache.ValueWrapper;

/**
 * 
 * @author Carlo Cancellieri - GeoSolutions SAS
 * 
 */
class CacheStatus {

    public static final String CACHEMANAGER_STORE_NAME = "CacheManagerStatus";

    // storage for the cache status
    private final transient static org.springframework.cache.Cache ehcache = EHCacheUtils
            .getCacheUtils().getCache(CACHEMANAGER_STORE_NAME);

    private final transient Logger LOGGER = org.geotools.util.logging.Logging.getLogger(getClass()
            .getPackage().getName());

    // operations
    private final transient ReadWriteLock cachedOpSPIMapLock = new ReentrantReadWriteLock();

    private final transient Map<Operation, CachedOpSPI<?>> cachedOpSPIMap = new HashMap<Operation, CachedOpSPI<?>>();

    // name for the operations map storage
    private final String uid;

    public CacheStatus(final String uid) {
        if (uid == null || uid.isEmpty())
            throw new IllegalArgumentException(
                    "Unable to create the cache status with null or empty uid");

        this.uid = uid;

        // try to restore from a previous status
        load();
    }

    public String getUID() {
        return this.uid;
    }

    Set<Operation> getCachedOpKeys() {
        try {
            this.cachedOpSPIMapLock.readLock().lock();
            return cachedOpSPIMap.keySet();
        } finally {
            this.cachedOpSPIMapLock.readLock().unlock();
        }
    }

    Collection<CachedOpSPI<?>> getCachedOps() {
        try {
            this.cachedOpSPIMapLock.readLock().lock();
            return cachedOpSPIMap.values();
        } finally {
            this.cachedOpSPIMapLock.readLock().unlock();
        }
    }

    /**
     * set the new spi collection comparing the stored set of SPI with the passed one and returning a collection of operation which are changed (this
     * should be used to clear the matching cachedOp before a new one is created into the CacheManager)
     * 
     * @param spiColl
     * @return
     */
    Collection<Operation> setCachedOpSPI(Collection<CachedOpSPI<?>> spiColl) {
        final List<Operation> returns = new ArrayList<Operation>();
        try {
            this.cachedOpSPIMapLock.writeLock().lock();

            // for each selected SPI changes
            final Iterator<CachedOpSPI<?>> it = spiColl.iterator();
            while (it.hasNext()) {
                final CachedOpSPI<?> selectedOpSPI = it.next();
                // check stored to compare with selected SPI
                if (cachedOpSPIMap.containsKey(selectedOpSPI.getOp())) {
                    final CachedOpSPI<?> storedOpSPI = cachedOpSPIMap.get(selectedOpSPI.getOp());
                    if (selectedOpSPI.getOp().equals(storedOpSPI.getOp())) {
                        // if stored is equals with the selected no change is required
                        if (!selectedOpSPI.equals(storedOpSPI)) {
                            // substitute the the stored operation with the new one
                            cachedOpSPIMap.put(selectedOpSPI.getOp(), selectedOpSPI);
                            // add the operation to the return list
                            returns.add(storedOpSPI.getOp());
                        }
                    }
                } else {
                    // the selected SPI is not present into the store map, let's add it
                    cachedOpSPIMap.put(selectedOpSPI.getOp(), selectedOpSPI);
                }
            }

            // now some SPI into the cachedOpSPIMap may be absent into the selection (unselected)
            // lets remove them from the stored map adding them to the returns list.
            final Iterator<Entry<Operation, CachedOpSPI<?>>> it3 = cachedOpSPIMap.entrySet()
                    .iterator();
            while (it3.hasNext()) {
                final Entry<Operation, CachedOpSPI<?>> entry = it3.next();
                final Operation op = entry.getKey();
                // if the selected operationSPI collection does not contains some SPI in the stored map
                boolean found = false;
                final Iterator<CachedOpSPI<?>> it4 = spiColl.iterator();
                while (it4.hasNext()) {
                    final CachedOpSPI<?> selectedOpSPI = it4.next();
                    if (selectedOpSPI.getOp().equals(op)) {
                        found = true;
                        continue;
                    }
                }
                if (!found) {
                    // add to the change list
                    returns.add(op);
                    // remove from the store
                    it3.remove();
                }
            }

            // final Iterator<Entry<Operation, CachedOpSPI<?>>> it2 = cachedOpSPIMap.entrySet()
            // .iterator();
            // while (it2.hasNext()) {
            // final Entry<Operation, CachedOpSPI<?>> op = it2.next();
            // final CachedOpSPI<?> storedOpSPI = op.getValue();
            // // for the same operation
            // if (selectedOpSPI.getOp().equals(storedOpSPI.getOp())) {
            // // if stored is equals with the selected no change is required
            // if (selectedOpSPI.equals(storedOpSPI)) {
            // continue;
            // } else {
            // // substitute the the stored operation with the new one
            // op.setValue(selectedOpSPI);
            // // add the operation to the return list
            // returns.add(storedOpSPI.getOp());
            // }
            // // remove from changes list
            // it.remove();
            // break;
            // }
            // }
            // }

 

            // now add remaining selected SPI
//            for (CachedOpSPI<?> spi : spiColl) {
//
//            }
        } finally {
            this.cachedOpSPIMapLock.writeLock().unlock();
        }
        return returns;
    }

    CachedOpSPI<?> getCachedOpSPI(CachedOpSPI<?> op) {
        try {
            this.cachedOpSPIMapLock.readLock().lock();
            return cachedOpSPIMap.get(op);
        } finally {
            this.cachedOpSPIMapLock.readLock().unlock();
        }
    }

    /**
     * loads the cachedOpSPIMap status from the ehcache
     */
    void load() {
        final ValueWrapper cachedStatus = ehcache.get(getUID());
        if (cachedStatus != null) {
            this.cachedOpSPIMap.putAll((Map<Operation, CachedOpSPI<?>>) cachedStatus.get());
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.log(Level.WARNING, "No entry load from cache for key: " + getUID());
            }
        }
    }

    // public void putCachedOp(CachedOpSPI<?> spi, CachedOp<?, ?> cachedOp) {
    // try {
    // this.cachedOpMapLock.writeLock().lock();
    // cachedOpMap.put(spi.getOp(), cachedOp);
    // } finally {
    // this.cachedOpMapLock.writeLock().unlock();
    // }
    // try {
    // this.cachedOpSPIMapLock.writeLock().lock();
    // cachedOpSPIMap.put(spi.getOp(),spi);
    // } finally {
    // this.cachedOpSPIMapLock.writeLock().unlock();
    // }
    // }

    // private void putAllCachedOp(Map<Operation, CachedOp<?, ?>> all) {
    // try {
    // this.cachedOpMapLock.writeLock().lock();
    // for (Entry<Operation, CachedOp<?, ?>> e : all.entrySet()) {
    // cachedOpMap.put(e.getKey(), e.getValue());
    // }
    // } finally {
    // this.cachedOpMapLock.writeLock().unlock();
    // }
    // }

    // public void putAllCachedOp(Map<CachedOpSPI<CachedOp<?, ?>>, CachedOp<?, ?>> all) {
    // try {
    // this.cachedOpMapLock.writeLock().lock();
    // for (Entry<CachedOpSPI<CachedOp<?, ?>>, CachedOp<?, ?>> e : all.entrySet()) {
    // cachedOpMap.put(e.getKey(), e.getValue());
    // }
    // } finally {
    // this.cachedOpMapLock.writeLock().unlock();
    // }
    // }

    /**
     * Recursively save the current status
     * 
     * @throws IOException
     */
    void save() throws IOException {
        try {
            this.cachedOpSPIMapLock.readLock().lock();
            // store the set into the cache
            ehcache.put(uid, cachedOpSPIMap);
        } finally {
            this.cachedOpSPIMapLock.readLock().unlock();
        }
    }

    /**
     * clear the cache status and all of the sub caches
     */
    void clear() throws IOException {
        // evict
        ehcache.evict(uid);
    }

    /**
     * dispose the operation<br/>
     * <b>NOTE:</b> {@link CacheStatus#save()} or {@link CacheStatus#clear()} should be called separately (before this)
     * 
     * @throws IOException
     */
    public void dispose() throws IOException {
    }

}
