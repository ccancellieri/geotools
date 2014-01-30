package org.geotools.data.cache.op;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.DataStore;
import org.springframework.beans.factory.DisposableBean;

public class CacheManager implements DisposableBean {
    private final transient Logger LOGGER = org.geotools.util.logging.Logging.getLogger(getClass()
            .getPackage().getName());

    // datastore
    private final transient DataStore source;

    // cache
    private final transient DataStore cache;

    private final CacheStatus status;

    private final String uid;

    // operations
    private final transient ReadWriteLock cachedOpMapLock = new ReentrantReadWriteLock();

    private final transient Map<Operation, CachedOp<?, ?>> cachedOpMap = new HashMap<Operation, CachedOp<?, ?>>();

    public CacheManager(DataStore store, DataStore cache, final String uid) {
        this(store, cache, uid, null);
    }

    public CacheManager(DataStore store, DataStore cache, final String uid,
            final Map<String, CachedOpSPI<?>> spiParams) {
        if (store == null || cache == null || uid == null)
            throw new IllegalArgumentException(
                    "Unable to create the cache manager with null source or cache datastore");
        this.source = store;
        this.cache = cache;
        this.uid = uid;
        // create a status configuration
        status = new CacheStatus(uid);

        // LOAD
        Collection<Operation> clear = null;
        if (spiParams != null) {
            clear = status.setCachedOpSPI(spiParams.values());
        }
        // re load the operations
        load(clear);
    }

    public String getUID() {
        return uid;
    }

    public CacheStatus getStatus() {
        return status;
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

    /**
     * Recursively save the operations and the status
     * 
     * @throws IOException
     */
    public void save() throws IOException {
        try {
            this.cachedOpMapLock.readLock().lock();
            // recursive save
            for (CachedOp<?, ?> op : cachedOpMap.values()) {
                op.save();
            }
        } finally {
            this.cachedOpMapLock.readLock().unlock();
        }

        // save also the status
        status.save();
    }

    public <K, T> void store(K key, T value) {
        status.store(key, value);
    }

    public <K, T> T load(K key) {
        return status.load(key);
    }

    public <K> void evict(K key) {
        status.evict(key);
    }

    private String createCachedOpUID(Operation op) {
        return new StringBuilder(op.toString()).append(':').append(uid).toString();
    }

    public void clear() throws IOException {
        try {
            this.cachedOpMapLock.readLock().lock();
            // recursively clear
            for (CachedOp<?, ?> op : cachedOpMap.values()) {
                try {
                    op.clear();
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, e.getMessage(), e);
                }
            }
        } finally {
            this.cachedOpMapLock.readLock().unlock();
        }

        // clear also the status
        this.status.clear();
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

        // dispose and remove stored operations
        try {
            this.cachedOpMapLock.writeLock().lock();
            final Iterator<Entry<Operation, CachedOp<?, ?>>> it = cachedOpMap.entrySet().iterator();
            while (it.hasNext()) {
                final Entry<Operation, CachedOp<?, ?>> op = it.next();
                // if the operation should be cleared
                if (clearCollection != null && clearCollection.contains(op.getKey())) {
                    op.getValue().clear();
                } else {
                    op.getValue().save();
                }
                op.getValue().dispose();
                it.remove();
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
        } finally {
            this.cachedOpMapLock.writeLock().unlock();
        }

        final Collection<CachedOpSPI<?>> spiList = status.getCachedOps();
        for (CachedOpSPI<?> spi : spiList) {
            try {
                // this will create the operation which may be able to load itself configuration
                final BaseOp<?, ?> op = spi.create(this, createCachedOpUID(spi.getOp()));
                // add this operation to the map
                try {
                    this.cachedOpMapLock.writeLock().lock();
                    cachedOpMap.put(spi.getOp(), op);
                } finally {
                    this.cachedOpMapLock.writeLock().unlock();
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, e.getMessage(), e);
            }
        }
    }

    public DataStore getSource() {
        return source;
    }

    public DataStore getCache() {
        return cache;
    }

    public void dispose() throws IOException {
        // save the status
         save();

        // dispose
        try {
            this.cachedOpMapLock.readLock().lock();
            for (CachedOp<?, ?> op : cachedOpMap.values()) {
                op.dispose();
            }
        } finally {
            this.cachedOpMapLock.readLock().unlock();
        }

        if (status != null) {
            status.dispose();
        }
        if (source != null) {
            source.dispose();
        }
        if (cache != null) {
            cache.dispose();
        }
    }

    @Override
    public void destroy() throws Exception {
        dispose();
    }

}
