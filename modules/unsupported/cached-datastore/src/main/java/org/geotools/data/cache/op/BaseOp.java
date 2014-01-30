package org.geotools.data.cache.op;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

public abstract class BaseOp<T, K> implements CachedOp<T, K> {
    protected final transient Logger LOGGER = org.geotools.util.logging.Logging
            .getLogger(getClass().getPackage().getName());

    // manager
    protected final transient CacheManager cacheManager;

    // UID for this instance
    protected String uid;

    // status of this operation
    protected final Map<K, Boolean> isCachedMap = new HashMap<K, Boolean>();

    // lock
    protected final transient ReadWriteLock isCachedLock = new ReentrantReadWriteLock();

    public BaseOp(CacheManager cacheManager, final String uid) throws IOException {

        if (uid == null || uid.isEmpty() || cacheManager == null)
            throw new IllegalArgumentException(
                    "Unable to build a CachedOp without the cacheManager or the unique name");

        this.cacheManager = cacheManager;

        // LOAD
        final BaseOp<T, K> baseOp = load(uid);
        if (baseOp != null) {
            clone(baseOp);
        } else {
            this.uid = uid;
        }
    }

    @Override
    public <E extends CachedOp<T, K>> void clone(E obj) throws IOException {
        final BaseOp<T, K> baseOp = (BaseOp<T, K>) obj;
        this.uid = baseOp.uid;
        this.isCachedMap.putAll(baseOp.isCachedMap);
    }

    public String getUid() {
        return uid;
    }

    @Override
    public void dispose() throws IOException {
    }

    @Override
    public Serializable save() throws IOException {
        cacheManager.store(uid, this);
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
    public <E extends CachedOp<T, K>> E load(Serializable uuid) throws IOException {
        verify(uuid);
        return cacheManager.load(uuid);
    }

    protected Map<K, Boolean> getIsCachedMap() {
        return isCachedMap;
    }

    @Override
    public boolean isCached(K o) throws IOException {
        try {
            isCachedLock.readLock().lock();
            final Boolean b = isCachedMap.get(o);
            return b != null ? b.booleanValue() : false;
        } finally {
            isCachedLock.readLock().unlock();
        }
    }

    @Override
    public void setCached(K key, boolean value) throws IOException {
        try {
            isCachedLock.writeLock().lock();
            isCachedMap.put(key, value);
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
