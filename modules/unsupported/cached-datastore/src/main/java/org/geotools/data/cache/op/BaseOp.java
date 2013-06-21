package org.geotools.data.cache.op;

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
    
    protected final static transient org.springframework.cache.Cache ehache=EHCacheUtils.getCacheUtils().getCache(CACHEDOP_STORE_NAME);
    
    // cached datastore
    protected final transient DataStore cache;

    // cached datastore
    protected final transient DataStore source;

    // manager
    protected final transient CacheManager cacheManager;
    
    // UID for this instance
    protected String uid;

    // status of this operation
    protected Map<Integer, Boolean> isCached;
    
    // lock
    protected final transient ReadWriteLock isCachedLock=new ReentrantReadWriteLock();

    public String getUid() {
        return uid;
    }

    @Override
    public void dispose() {
        // if (source!=null)
        // source.dispose();
        if (cache != null)
            cache.dispose();
    }

    @Override
    public String save() {
        ehache.put(uid, isCached);
        return uid;
    }

    @Override
    public void load(String obj) {
        if (obj==null || obj.isEmpty()){
            throw new IllegalArgumentException("Unable to load using a null or empty UID");
        }
        uid=obj;
        ValueWrapper isCachedObj = ehache.get(obj);
        try {
            isCachedLock.writeLock().lock();
            if (isCachedObj != null) {
                isCached = (Map<Integer, Boolean>) isCachedObj.get();
            } else {
                LOGGER.warning("No cached status is found");
                isCached = new HashMap<Integer, Boolean>();
            }
        } finally {
            isCachedLock.writeLock().unlock();
        }
    }

    @Override
    public boolean isCached(K... o) {
        try {
            isCachedLock.readLock().lock();
            Object b = isCached.get(Arrays.deepHashCode(o));
            return b != null ? (Boolean) b : false;
        } finally {
            isCachedLock.readLock().unlock();
        }
    }

    @Override
    public void setCached(boolean isCached, K... key) {
        try {
            isCachedLock.writeLock().lock();
            this.isCached.put(Arrays.deepHashCode(key), isCached);
        } finally {
            isCachedLock.writeLock().unlock();
        }
    }

    public BaseOp(CacheManager cacheManager, final String uniqueName) {
        if (uniqueName==null || uniqueName.isEmpty() || cacheManager==null)
            throw new IllegalArgumentException("Unable to build a CachedOp without the cacheManager or the unique name");
        this.cacheManager = cacheManager;
        this.cache = cacheManager.getCache();
        this.source = cacheManager.getSource();
        
        // load
        load(uniqueName);
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

    // @Override
    // public T getCache(C ... key) throws IOException {
    // T op = null;
    // if (!isCached(key)) {
    // op = operation(o);
    // setCached(key, cache(op));
    // }
    // if (isCached(key)) {
    // return getCachedInternal(key, o);
    // } else {
    // return op;
    // }
    // }
    //
    // protected abstract T getCachedInternal(K key, C o) throws IOException;

}
