package org.geotools.data.cache.op;

import java.io.IOException;
import java.io.Serializable;

/**
 * 
 * @author carlo
 * 
 * @param <T> The type returned by the cachedOp(eration)
 * @param <K> the type used to check if the operation has cached a specific call
 */
public interface CachedOp<T, K> {

    /**
     * @return a string representing the status of this cachedOp object (note: this should be loadable by the {@link #load(String)} method)
     * @throws IOException
     */
    public Serializable save() throws IOException;

    /**
     * load the status of this cachedOp using the input Serializable (if needed) to setup accordingly the status of this object
     * 
     * @param obj the status to load
     */
    public void load(Serializable obj) throws IOException;

    /**
     * clear the status of this cachedOp
     * 
     * @throws IOException
     */
    public void clear() throws IOException;

    /**
     * this is a dispose method (should be called when this object is no more used)
     * @throws IOException 
     */
    public void dispose() throws IOException;

    /**
     * @param key
     * @param o
     * @return the cached object
     * @throws IOException
     */
    public T getCache(K o) throws IOException;

    /**
     * perform a cache update
     * 
     * @param arg the object to cache
     * @return true if success
     * @throws IOException
     */
    public T updateCache(K key) throws IOException;

    public boolean isDirty(K key) throws IOException;

    void setDirty(K key) throws IOException;

    /**
     * @return true if cache was already called and cached value is not dirty
     * @throws IOException
     */
    public boolean isCached(K key) throws IOException;

    /**
     * Should be used to set cached objects or invalidate them (dirty)
     * 
     * @param key
     * @param isCached
     * @throws IOException
     */
    public void setCached(K key, boolean isCached) throws IOException;

}
