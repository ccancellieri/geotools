package org.geotools.data.cache.op;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import org.geotools.data.DataStore;
import org.geotools.data.cache.utils.EHCacheUtils;
import org.springframework.cache.Cache.ValueWrapper;

public abstract class BaseOp<T, C, K> implements CachedOp<T, C, K> {
    protected final transient Logger LOGGER = org.geotools.util.logging.Logging
            .getLogger(getClass().getPackage().getName());

    public final static String CACHEDOP_STORE_NAME = "CachedOpStatus";

    protected final static transient org.springframework.cache.Cache ehache = EHCacheUtils
            .getCacheUtils().getCache(CACHEDOP_STORE_NAME);

    // cached datastore
    protected final transient DataStore cache;

    // cached datastore
    protected final transient DataStore source;

    // manager
    protected final transient CacheManager cacheManager;

    // UID for this instance
    protected final String uid;

    // status of this operation
    protected Map<Integer, Boolean> isCachedMap;

    // lock
    protected final transient ReadWriteLock isCachedLock = new ReentrantReadWriteLock();

    public BaseOp(CacheManager cacheManager, final String uid) {
        if (uid == null || uid.isEmpty() || cacheManager == null)
            throw new IllegalArgumentException(
                    "Unable to build a CachedOp without the cacheManager or the unique name");
        this.cacheManager = cacheManager;
        this.cache = cacheManager.getCache();
        this.source = cacheManager.getSource();
        this.uid = uid;
    }

    public String getUid() {
        return uid;
    }

    @Override
    public void dispose() {
        if (cache != null)
            cache.dispose();
    }

    @Override
    public Serializable[] save() {
        ehache.put(uid, isCachedMap);
        return new String[] { uid };
    }

    @Override
    public void clear() {
        try {
            isCachedLock.writeLock().lock();
            isCachedMap.clear();
        } finally {
            isCachedLock.writeLock().unlock();
        }
        ehache.evict(uid);
    }

    @Override
    public void load(Serializable... obj) {

        // in this implementation input string is not used
        // if (obj==null || obj.isEmpty()){
        // throw new IllegalArgumentException("Unable to load using a null or empty UID");
        // }

        ValueWrapper isCachedObj = ehache.get(uid);
        try {
            isCachedLock.writeLock().lock();
            if (isCachedObj != null) {
                isCachedMap = (Map<Integer, Boolean>) isCachedObj.get();
            } else {
                LOGGER.warning("No cached status is found");
                isCachedMap = new HashMap<Integer, Boolean>();
            }
        } finally {
            isCachedLock.writeLock().unlock();
        }
    }

    @Override
    public boolean isCached(K... o) {
        try {
            isCachedLock.readLock().lock();
            Object b = isCachedMap.get(Arrays.deepHashCode(o));
            return b != null ? (Boolean) b : false;
        } finally {
            isCachedLock.readLock().unlock();
        }
    }

    @Override
    public void setCached(boolean isCached, K... key) {
        try {
            isCachedLock.writeLock().lock();
            this.isCachedMap.put(Arrays.deepHashCode(key), isCached);
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

}
