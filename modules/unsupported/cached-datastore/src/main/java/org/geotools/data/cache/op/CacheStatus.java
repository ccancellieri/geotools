package org.geotools.data.cache.op;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import org.geotools.data.cache.utils.EHCacheUtils;
import org.springframework.cache.Cache.ValueWrapper;

public class CacheStatus {

    private final transient Logger LOGGER = org.geotools.util.logging.Logging.getLogger(getClass()
            .getPackage().getName());
    
    public static final String CACHEMANAGER_STORE_NAME = "CacheManagerStatus";
    
    // storage for the cache status
    private final transient static org.springframework.cache.Cache ehcache = EHCacheUtils.getCacheUtils().getCache(CACHEMANAGER_STORE_NAME);

    private final transient ReadWriteLock cacheMapLock = new ReentrantReadWriteLock();

    // operations
    private Map<CachedOpSPI<?>, CachedOp<?, ?, ?>> cacheMap;
    

    // name for the operations map storage
    private String uid;

    public CacheStatus(final String uid) {
        if (uid == null || uid.isEmpty())
            throw new IllegalArgumentException(
                    "Unable to create the cache status with null or empty uid");

        // perform status initialization
        load(uid);
    }

    public String getUID() {
        return uid;
    }

    public Set<CachedOpSPI<?>> getCachedOpKeys() {
        try {
            this.cacheMapLock.readLock().lock();
            return getCacheMap().keySet();
        } finally {
            this.cacheMapLock.readLock().unlock();
        }
    }

    public CachedOp<?, ?, ?> getCachedOp(CachedOpSPI<?> op) {
        try {
            this.cacheMapLock.readLock().lock();
            return (CachedOp<?, ?, ?>) getCacheMap().get(op);
        } finally {
            this.cacheMapLock.readLock().unlock();
        }
    }

    /**
     * @see {@link CachedOpSPI#hashCode()}
     * @param op
     * @return
     */
    public CachedOp<?, ?, ?> getCachedOp(Operation op) {
        try {
            this.cacheMapLock.readLock().lock();
            return (CachedOp<?, ?, ?>) getCacheMap().get(op);
        } finally {
            this.cacheMapLock.readLock().unlock();
        }
    }

    public void putCachedOp(CachedOpSPI<?> spi, CachedOp<?, ?, ?> cachedOp) {
        try {
            this.cacheMapLock.writeLock().lock();
            getCacheMap().put(spi, cachedOp);
        } finally {
            this.cacheMapLock.writeLock().unlock();
        }
    }

    public void putAllCachedOp(Map<CachedOpSPI<?>, CachedOp<?, ?, ?>> all) {
        try {
            this.cacheMapLock.writeLock().lock();
            for (Entry<CachedOpSPI<?>, CachedOp<?, ?, ?>> e : all.entrySet()) {
                getCacheMap().put(e.getKey(), e.getValue());
            }
        } finally {
            this.cacheMapLock.writeLock().unlock();
        }
    }

    protected Map<CachedOpSPI<?>, CachedOp<?, ?, ?>> getCacheMap() {
        return cacheMap;
    }

    /**
     * @return the internal status as string
     */
    public String save() {
        return uid;
    }

    public void load(String uid) {
        // set passed uid
        this.uid = uid;

        try {
            cacheMapLock.writeLock().lock();
            if (this.cacheMap == null) {
                ValueWrapper map = ehcache.get(getUID());
                if (map != null) {
                    this.cacheMap = (Map<CachedOpSPI<?>, CachedOp<?, ?, ?>>) map.get();
                } else {
                    this.cacheMap = new HashMap<CachedOpSPI<?>, CachedOp<?, ?, ?>>();
                    // store map into the cache
                    ehcache.put(uid, this.cacheMap);
                }
            }
        } finally {
            // release the write lock
            cacheMapLock.writeLock().unlock();
        }
    }

    public void dispose() {
        if (this.cacheMap != null) {
            for (CachedOp<?, ?, ?> op : cacheMap.values()) {
                op.save();
                op.dispose();
            }
        }

    }

    public String createCachedOpUID(Operation op) {
        return new StringBuilder(op.toString()).append(':').append(uid).toString();
    }

}
