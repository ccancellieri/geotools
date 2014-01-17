package org.geotools.data.cache.op;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import org.geotools.data.cache.utils.EHCacheUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

public abstract class BaseOp<T, K> implements CachedOp<T, K> {
    protected final transient Logger LOGGER = org.geotools.util.logging.Logging
            .getLogger(getClass().getPackage().getName());

    public final static String CACHEDOP_STORE_NAME = "CachedOpStatus";

    protected final static transient org.springframework.cache.Cache ehCache = EHCacheUtils
            .getCacheUtils().getCache(CACHEDOP_STORE_NAME);

    // lock on cache
    protected final static transient ReadWriteLock lockCache = new ReentrantReadWriteLock();

    // manager
    protected final transient CacheManager cacheManager;

    // UID for this instance
    protected final String uid;

    // status of this operation
    protected final Map<K, Boolean> isCachedMap = new HashMap<K, Boolean>();

    // lock
    protected final transient ReadWriteLock isCachedLock = new ReentrantReadWriteLock();

    public BaseOp(CacheManager cacheManager, final String uid) {

        if (uid == null || uid.isEmpty() || cacheManager == null)
            throw new IllegalArgumentException(
                    "Unable to build a CachedOp without the cacheManager or the unique name");

        this.cacheManager = cacheManager;

        this.uid = uid;

    }

    /**
     * copy constructor
     * 
     * @param op
     */
    public BaseOp(BaseOp op) {
        this(op.cacheManager, op.uid);
    }

    public String getUid() {
        return uid;
    }

    @Override
    public void dispose() throws IOException {
        
        save();
    }

    @Override
    public Serializable save() throws IOException {
        try {
            lockCache.writeLock().lock();

            ehCachePut(ehCache, this, uid);
        } finally {
            lockCache.writeLock().unlock();
        }
        return uid;
    }

    @Override
    public void clear() throws IOException {
        try {
            isCachedLock.writeLock().lock();
            isCachedMap.clear();
        } finally {
            isCachedLock.writeLock().unlock();
        }
    }

    @Override
    public void load(Serializable uuid) {
        verify(uuid);
        Object serialized = ehCache.get(uuid);
        if (serialized != null) {
            BeanUtils.copyProperties(serialized, this);
        } else {
            LOGGER.info("No stored status is found for this operation: " + uuid);
        }

    }

    @Override
    public boolean isCached(K o) throws IOException {
        try {
            isCachedLock.readLock().lock();
            Object b = isCachedMap.get(o);
            return b != null ? true : false;
        } finally {
            isCachedLock.readLock().unlock();
        }
    }

    @Override
    public void setCached(K key, boolean value) throws IOException {
        try {
            isCachedLock.writeLock().lock();
            this.isCachedMap.put(key, value);
        } finally {
            isCachedLock.writeLock().unlock();
        }
    }

    /**
     * can be used to check the input: will throws {@link IllegalArgumentException} if o==null or o.length<0
     * 
     * @param o
     * @throws IllegalArgumentException
     */
    protected static <Y> void verify(Y... o) throws IllegalArgumentException {
        if (o == null || o.length < 1) {
            throw new IllegalArgumentException("wrong argument passed");
        }
    }

    protected static <T> void ehCachePut(Cache ehCacheManager, T value, Object... keys)
            throws IOException {
        verify(ehCacheManager);
        verify(value);
        verify(keys);

        if (value != null) {
            ehCacheManager.put(Arrays.deepHashCode(keys), value);
        } else {
            throw new IOException(
                    "Unable to cache a null Object, please check the source datastore.");
        }
    }

    protected static <T> T ehCacheGet(Cache cacheManager, Object... keys) {
        verify(cacheManager);
        verify(keys);
        final SimpleValueWrapper vw = (SimpleValueWrapper) cacheManager.get(Arrays
                .deepHashCode(keys));
        if (vw != null) {
            return (T) vw.get();
        } else {
            return null;
        }
    }

}
