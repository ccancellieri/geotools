package org.geotools.data.cache.op;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class BaseOpStatus<K> implements CachedOpStatus<K> {

    // UID for this instance
    protected String uid;

    // status of this operation
    protected final Map<K, Boolean> isCachedMap = new HashMap<K, Boolean>();

    // lock
    protected final transient ReadWriteLock isCachedLock = new ReentrantReadWriteLock();

    protected final Map<K, Boolean> isDirty = new HashMap<K, Boolean>();

    // lock
    protected final transient ReentrantReadWriteLock isDirtyLock = new ReentrantReadWriteLock();

    protected Map<K, Boolean> getIsCachedMap() {
        return isCachedMap;
    }

    public String getUid() {
        return uid;
    }

    // @Override
    // public Serializable save() throws IOException {
    // EHCacheUtils.store(uid, this);
    // return uid;
    // }

    @Override
    public void clear() throws IOException {
        try {
            isCachedLock.writeLock().lock();
            isCachedMap.clear();
        } finally {
            isCachedLock.writeLock().unlock();
        }
        try {
            isDirtyLock.writeLock().lock();
            isDirty.clear();
        } finally {
            isDirtyLock.writeLock().unlock();
        }
    }

    // @Override
    // public <E extends BaseOpStatus<T, K>> E load(Serializable uuid) throws IOException {
    // verify(uuid);
    // // LOAD
    // final BaseOpStatus<T, K> baseOp = EHCacheUtils.load(uuid);
    // if (baseOp != null) {
    // clone(baseOp);
    // } else {
    // this.uid = (String)uuid;
    // }
    // return (E) this;
    // }

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

    @Override
    public boolean isDirty(K key) throws IOException {
        try {
            isDirtyLock.readLock().lock();
            final Boolean b = isDirty.get(key);
            return b != null ? b.booleanValue() : false;
        } finally {
            isDirtyLock.readLock().unlock();
        }

    }

    @Override
    public void setDirty(K name, boolean value) throws IOException {
        try {
            isDirtyLock.writeLock().lock();
            isDirty.put(name, value);
        } finally {
            isDirtyLock.writeLock().unlock();
        }
    }

}
