package org.geotools.data.cache.op;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.DataStore;
import org.geotools.data.cache.utils.CacheUtils;
import org.geotools.data.cache.utils.EHCacheUtils;

public class CacheManager {
    private final transient Logger LOGGER = org.geotools.util.logging.Logging.getLogger(getClass()
            .getPackage().getName());

    // datastore
    private final transient DataStore source;

    // cache
    private final transient DataStore cache;

    private final CacheStatus status;

    public CacheManager(DataStore store, DataStore cache, final String uid) {
        if (store == null || cache == null)
            throw new IllegalArgumentException(
                    "Unable to create the cache manager with null source or cache datastore");
        this.source = store;
        this.cache = cache;

        // create a status configuration
        status = new CacheStatus(uid);

        // effectively loads the cachedOps into the status.
        load(uid);
    }

    // public String getUID() {
    // return uid;
    // }

    // public Set<CachedOpSPI<?>> getCachedOpKeys() {
    // try {
    // this.cacheMapLock.readLock().lock();
    // return getCacheMap().keySet();
    // } finally {
    // this.cacheMapLock.readLock().unlock();
    // }
    // }
    //
    // public CachedOp<?, ?, ?> getCachedOp(CachedOpSPI<?> op) {
    // try {
    // this.cacheMapLock.readLock().lock();
    // return (CachedOp<?, ?, ?>) getCacheMap().get(op);
    // } finally {
    // this.cacheMapLock.readLock().unlock();
    // }
    // }
    //
    /**
     * @see {@link CachedOpSPI#hashCode()}
     * @param op
     * @return
     */
    public CachedOp<?, ?, ?> getCachedOp(Operation op) {
        return status.getCachedOp(op);
    }

    //
    // public void putCachedOp(CachedOpSPI<?> spi, CachedOp<?, ?, ?> cachedOp) {
    // try {
    // this.cacheMapLock.writeLock().lock();
    // getCacheMap().put(spi, cachedOp);
    // } finally {
    // this.cacheMapLock.writeLock().unlock();
    // }
    // }
    //
    // public void putAllCachedOp(Map<CachedOpSPI<?>, CachedOp<?, ?, ?>> all) {
    // try {
    // this.cacheMapLock.writeLock().lock();
    // for (Entry<CachedOpSPI<?>, CachedOp<?, ?, ?>> e : all.entrySet()) {
    // getCacheMap().put(e.getKey(), e.getValue());
    // }
    // } finally {
    // this.cacheMapLock.writeLock().unlock();
    // }
    // }
    //
    // protected Map<CachedOpSPI<?>, CachedOp<?, ?, ?>> getCacheMap() {
    // if (this.cacheMap == null) {
    // init(false);
    // }
    // return cacheMap;
    // }

    /**
     * @return the internal status as string
     */
    public String save() {
        // do nothing (changes are automatically stored by ehcache)
        return status.getUID();
    }

    public void load(String uniqueName) {
        final CacheUtils cu = CacheUtils.getCacheUtils();
        if (cu != null) {
            for (Operation op : Operation.values()) {
                for (CachedOpSPI<?> spi : cu.getCachedOps()) {
                    if (spi.getOp().equals(op)) {
                        try {
                            CachedOp<?, ?, ?> cOp = spi.create(source, cache, this,
                                    createCachedOpUID(op));
                            this.status.putCachedOp(spi, cOp);
                        } catch (IOException e) {
                            LOGGER.log(Level.SEVERE, e.getMessage(), e);
                        }
                    }
                }
            }
        } else {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.log(Level.SEVERE, "Unable to load " + CacheUtils.class);
            }
        }
    }

    public DataStore getSource() {
        return source;
    }

    public DataStore getCache() {
        return cache;
    }

    public void dispose() {
        if (status != null) {
            status.dispose();
        }
        if (source != null)
            source.dispose();
        if (cache != null)
            cache.dispose();

    }

    public String createCachedOpUID(Operation op) {
        return new StringBuilder(status.getUID()).append(op.toString()).toString();
    }
}
