package org.geotools.data.cache.op;

import java.io.IOException;
import java.sql.Savepoint;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.DataStore;
import org.geotools.data.cache.utils.EHCacheUtils;
import org.springframework.cache.Cache.ValueWrapper;

/**
 * 
 * @author Carlo Cancellieri - GeoSolutions SAS
 * 
 */
public class CacheStatus {

    public static final String CACHEMANAGER_STORE_NAME = "CacheManagerStatus";

    // storage for the cache status
    private final transient static org.springframework.cache.Cache ehcache = EHCacheUtils
            .getCacheUtils().getCache(CACHEMANAGER_STORE_NAME);

    private final transient Logger LOGGER = org.geotools.util.logging.Logging.getLogger(getClass()
            .getPackage().getName());

    private final transient ReadWriteLock cachedOpMapLock = new ReentrantReadWriteLock();

    // operations
    private final transient Map<Operation, CachedOp<?, ?, ?>> cachedOpMap = new HashMap<Operation, CachedOp<?, ?, ?>>();

    // name for the operations map storage
    private final String uid;

    public CacheStatus(final String uid) {
        if (uid == null || uid.isEmpty())
            throw new IllegalArgumentException(
                    "Unable to create the cache status with null or empty uid");

        this.uid = uid;
    }

    public String getUID() {
        return this.uid;
    }

    public Set<Operation> getCachedOpKeys() {
        try {
            this.cachedOpMapLock.readLock().lock();
            return cachedOpMap.keySet();
        } finally {
            this.cachedOpMapLock.readLock().unlock();
        }
    }

    public CachedOp<?, ?, ?> getCachedOp(CachedOpSPI<?> op) {
        try {
            this.cachedOpMapLock.readLock().lock();
            return (CachedOp<?, ?, ?>) cachedOpMap.get(op);
        } finally {
            this.cachedOpMapLock.readLock().unlock();
        }
    }

    /**
     * @see {@link CachedOpSPI#hashCode()}
     * @param op
     * @return
     */
    public CachedOp<?, ?, ?> getCachedOp(Operation op) {
        try {
            this.cachedOpMapLock.readLock().lock();
            return (CachedOp<?, ?, ?>) cachedOpMap.get(op);
        } finally {
            this.cachedOpMapLock.readLock().unlock();
        }
    }

    public void putCachedOp(Operation op, CachedOp<?, ?, ?> cachedOp) {
        try {
            this.cachedOpMapLock.writeLock().lock();
            cachedOpMap.put(op, cachedOp);
        } finally {
            this.cachedOpMapLock.writeLock().unlock();
        }
    }

    // public void putCachedOp(CachedOpSPI<CachedOp<?, ?, ?>> spi, CachedOp<?, ?, ?> cachedOp) {
    // try {
    // this.cachedOpMapLock.writeLock().lock();
    // cachedOpMap.put(spi, cachedOp);
    // } finally {
    // this.cachedOpMapLock.writeLock().unlock();
    // }
    // }

    public void putAllCachedOp(Map<Operation, CachedOp<?, ?, ?>> all) {
        try {
            this.cachedOpMapLock.writeLock().lock();
            for (Entry<Operation, CachedOp<?, ?, ?>> e : all.entrySet()) {
                cachedOpMap.put(e.getKey(), e.getValue());
            }
        } finally {
            this.cachedOpMapLock.writeLock().unlock();
        }
    }

    // public void putAllCachedOp(Map<CachedOpSPI<CachedOp<?, ?, ?>>, CachedOp<?, ?, ?>> all) {
    // try {
    // this.cachedOpMapLock.writeLock().lock();
    // for (Entry<CachedOpSPI<CachedOp<?, ?, ?>>, CachedOp<?, ?, ?>> e : all.entrySet()) {
    // cachedOpMap.put(e.getKey(), e.getValue());
    // }
    // } finally {
    // this.cachedOpMapLock.writeLock().unlock();
    // }
    // }

    /**
     * Recursively save the current status
     */
    public void save() {
        try {
            this.cachedOpMapLock.writeLock().lock();
            // recursive save
            for (CachedOp<?, ?, ?> op : cachedOpMap.values()) {
                op.save();
            }
            // store SPI set into the cache
            ehcache.put(uid, this.cachedOpMap.keySet());
        } finally {
            this.cachedOpMapLock.writeLock().unlock();
        }
    }

    /**
     * clear the cache status and all of the sub caches
     */
    public void clear() {
        try {
            this.cachedOpMapLock.writeLock().lock();
            // recursively clear
            for (CachedOp<?, ?, ?> op : cachedOpMap.values()) {
                op.clear();
            }
            // clear
            this.cachedOpMap.clear();
        } finally {
            this.cachedOpMapLock.writeLock().unlock();
        }
        // evict
        ehcache.evict(getUID());
    }

    /**
     * loads the cacheMap from the ehcache
     * 
     * @throws IOException
     */
    public void load(final CacheManager cacheManager) {
        if (cacheManager == null)
            throw new IllegalArgumentException(
                    "Unable to load the Status using a null cache manager");
        try {
            this.cachedOpMapLock.writeLock().lock();
            final ValueWrapper cachedSet = ehcache.get(getUID());
            if (cachedSet != null) {
                final Set<CachedOpSPI<CachedOp<?, ?, ?>>> spiSet = (Set<CachedOpSPI<CachedOp<?, ?, ?>>>) cachedSet
                        .get();
                // recursive load
                for (Iterator<CachedOpSPI<CachedOp<?, ?, ?>>> spiIt = spiSet.iterator(); spiIt
                        .hasNext();) {
                    CachedOpSPI<CachedOp<?, ?, ?>> spi = spiIt.next();
                    try {
                        loadOp(cacheManager, spi);
                    } catch (IOException e) {
                        LOGGER.log(Level.FINER, e.getMessage(), e);
                    }
                }
            } else {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.log(Level.WARNING, "No entry load from cache for key: " + getUID());
                }
            }
        } finally {
            // release the write lock
            this.cachedOpMapLock.writeLock().unlock();
        }
    }

    /**
     * Initialize and load a single operation using the passed SPI
     * 
     * @param source
     * @param cache
     * @param cacheManager
     * @param spi
     * @throws IOException if fails to create the CachedOp
     */
    public void loadOp(final CacheManager cacheManager, CachedOpSPI<CachedOp<?, ?, ?>> spi)
            throws IOException {
        if (cacheManager == null || spi == null)
            throw new IllegalArgumentException(
                    "Unable to load an operation using a null cache manager or a null spi");
        final CachedOp<?, ?, ?> op = spi.create(cacheManager, createCachedOpUID(spi.getOp()));
        // load op
        op.load(null);

        cachedOpMap.put(spi.getOp(), op);
    }

    /**
     * dispose the operation<br/>
     * <b>NOTE:</b> {@link CacheStatus#save()} or {@link CacheStatus#clear()} should be called separately (before this)
     */
    public void dispose() {
        try {
            this.cachedOpMapLock.writeLock().lock();
            if (this.cachedOpMap != null) {
                for (CachedOp<?, ?, ?> op : cachedOpMap.values()) {
                    op.dispose();
                }
            }
        } finally {
            this.cachedOpMapLock.writeLock().unlock();
        }
    }

    public String createCachedOpUID(Operation op) {
        return new StringBuilder(op.toString()).append(':').append(uid).toString();
    }

}
