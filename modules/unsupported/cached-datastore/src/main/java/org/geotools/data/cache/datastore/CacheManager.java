package org.geotools.data.cache.datastore;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.DataStore;
import org.geotools.data.cache.op.CachedOp;
import org.geotools.data.cache.op.CachedOpSPI;
import org.geotools.data.cache.op.CachedOpStatus;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.cache.utils.EHCacheUtils;

public class CacheManager {
    private final transient Logger LOGGER = org.geotools.util.logging.Logging.getLogger(getClass()
            .getPackage().getName());

    // datastore
    private final transient DataStore source;

    // cache
    private final transient DataStore cache;

    // status
    private final CacheStatus status;

    // operations
    private final transient ReadWriteLock cachedOpMapLock = new ReentrantReadWriteLock();

    // cached operations
    private final transient Map<Operation, CachedOp<?, ?>> cachedOpMap = new HashMap<Operation, CachedOp<?, ?>>();

    // public CacheManager(DataStore store, DataStore cache, final CacheStatus status) {
    // if (store == null || cache == null)
    // throw new IllegalArgumentException(
    // "Unable to create the cache manager with null source or cache datastore");
    // this.source = store;
    // this.cache = cache;
    // this.status = status;
    //
    // load(null);
    // }

    public CacheManager(DataStore store, DataStore cache, final String typeName,
            final Map<String, CachedOpSPI<CachedOpStatus<?>, CachedOp<?, ?>, ?, ?>> cachedOpSPIMap,
            final boolean sharedFeatureStatus) {
        if (store == null || cache == null || typeName == null)
            throw new IllegalArgumentException(
                    "Unable to create the cache manager with null source or cache datastore");
        this.source = store;
        this.cache = cache;
        // try to load
        final CacheStatus cs = EHCacheUtils.load(typeName);
        if (cs != null) {
            this.status = cs;
        } else {
            this.status = new CacheStatus(typeName, cachedOpSPIMap);
            // store created status
            EHCacheUtils.store(typeName, status);
        }

        load(null);
    }

    /**
     * Recursively save the current status
     * 
     * @throws IOException
     */
    public void save() throws IOException {
        EHCacheUtils.store(status.getUID(), status);
//        EHCacheUtils.flush();
    }

    /**
     * clear the cache status and all of the sub caches
     */
    public void clear() throws IOException {
        status.clear();
        // persist changes
        save();
        // flush
        EHCacheUtils.flush();
    }

    /**
     * Delegate to {@link CacheStatus#getCachedOp(CachedOpSPI)}
     * 
     * @param op
     * @return
     */
    public CachedOp<?, ?> getCachedOp(Operation op) {

        try {
            this.cachedOpMapLock.readLock().lock();
            return cachedOpMap.get(op);
        } finally {
            this.cachedOpMapLock.readLock().unlock();
        }

    }

    void putCachedOp(Operation op, CachedOp<?, ?> cachedOp) {
        try {
            this.cachedOpMapLock.writeLock().lock();
            cachedOpMap.put(op, cachedOp);
        } finally {
            this.cachedOpMapLock.writeLock().unlock();
        }
    }

    public <T> T getCachedOpOfType(Operation op, Class<T> clazz) {
        final Object obj = getCachedOp(op);
        if (obj == null)
            return null;
        if (clazz.isAssignableFrom(obj.getClass()))
            return clazz.cast(obj);
        else
            return null;
    }

    public <T> T getCachedOpOfType(Class<T> clazz) {

        for (CachedOp<?, ?> op : cachedOpMap.values()) {
            try {
                this.cachedOpMapLock.readLock().lock();
                if (clazz.isAssignableFrom(op.getClass())) {
                    return clazz.cast(op);
                }
            } finally {
                this.cachedOpMapLock.readLock().unlock();
            }
        }
        return null;
    }

    // private String createCachedOpUID(Operation op) {
    // return new StringBuilder(status.getUID()).append(':').append(op.toString()).toString();
    // }

    // public void save() throws IOException {
    // // save the status
    // EHCacheUtils.store(status.getUID(), status);
    //
    // // recursively save
    // try {
    // this.cachedOpMapLock.readLock().lock();
    // for (CachedOp<?, ?> op : cachedOpMap.values()) {
    // try {
    // op.save();
    // } catch (IOException e) {
    // LOGGER.log(Level.SEVERE, e.getMessage(), e);
    // }
    // }
    // } finally {
    // this.cachedOpMapLock.readLock().unlock();
    // }
    // }

    // public void clear() throws IOException {
    // try {
    // this.cachedOpMapLock.readLock().lock();
    // // recursively clear
    // for (CachedOp<?, ?> op : cachedOpMap.values()) {
    // try {
    // op.clear();
    // } catch (IOException e) {
    // LOGGER.log(Level.SEVERE, e.getMessage(), e);
    // }
    // }
    // } finally {
    // this.cachedOpMapLock.readLock().unlock();
    // }
    // }

    public void dispose() throws IOException {

        // dispose
        try {
            this.cachedOpMapLock.readLock().lock();
            // recursively clear
            for (CachedOp<?, ?> op : cachedOpMap.values()) {
                try {
                    op.dispose();
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, e.getMessage(), e);
                }
            }
        } finally {
            this.cachedOpMapLock.readLock().unlock();
        }
    }

    /**
     * loads from the cache the status of this {@link CacheManager} creating a new set of CachedOpSPI set from the status
     * 
     * @param uniqueName
     */

    public void load(Collection<Operation> clearCollection) {

        if (status == null)
            throw new IllegalArgumentException(
                    "Unable to load an operation using a null status cache manager");
        //
        // // dispose and remove stored operations
        // try {
        // this.cachedOpMapLock.writeLock().lock();
        // final Iterator<Entry<Operation, CachedOp<?, ?>>> it = cachedOpMap.entrySet().iterator();
        // while (it.hasNext()) {
        // final Entry<Operation, CachedOp<?, ?>> op = it.next();
        // // if the operation should be cleared
        // if (clearCollection != null && clearCollection.contains(op.getKey())) {
        // op.getValue().clear();
        // } else {
        // op.getValue().save();
        // }
        // op.getValue().dispose();
        // it.remove();
        // }
        // } catch (IOException e) {
        // LOGGER.log(Level.SEVERE, e.getMessage(), e);
        // } finally {
        // this.cachedOpMapLock.writeLock().unlock();
        // }

        final Collection<CachedOpSPI<CachedOpStatus<?>, CachedOp<?, ?>, ?, ?>> spiList = status
                .getCachedOps();
        for (CachedOpSPI<CachedOpStatus<?>, ?, ?, ?> spi : spiList) {
            try {
                final CachedOp op = createCachedOp(this, spi);
                // add this operation to the map
                if (op != null) {
                    try {
                        this.cachedOpMapLock.writeLock().lock();
                        cachedOpMap.put(spi.getOp(), op);
                    } finally {
                        this.cachedOpMapLock.writeLock().unlock();
                    }
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, e.getMessage(), e);
            }
        }
    }

    private static CachedOp createCachedOp(CacheManager manager,
            CachedOpSPI<CachedOpStatus<?>, ?, ?, ?> spi) throws IOException {
        // this will create the operation which may be able to load itself configuration
        final CachedOpStatus<?> cachedStatus = (CachedOpStatus<?>) manager.status
                .getCachedOpStatus(spi.getOp());
        return (CachedOp) spi.create(manager,
                cachedStatus != null ? cachedStatus : spi.createStatus());
    }

    public DataStore getSource() {
        return source;
    }

    public DataStore getCache() {
        return cache;
    }

    public CacheStatus getStatus() {
        return status;
    }

}
