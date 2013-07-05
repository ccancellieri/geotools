package org.geotools.data.cache.op;

import java.io.IOException;
import java.io.Serializable;

/**
 * 
 * @author carlo
 * 
 * @param <T> The type returned by the cachedOp(eration)
 * @param <C> the type used by the {@link CachedOp#putCache(Object...)} operation
 * @param <K> the type used to check if the operation has cached a specific call
 */
public interface CachedOp<T, C, K> {
    
    /**
     * @return a string representing the status of this cachedOp object (note: this should be loadable by the {@link #load(String)} method)
     */
    public Serializable[] save();
    
    /**
     * load the status of this cachedOp using the input string (if needed) to setup accordingly the status of this object
     * @param obj the status to load
     */
    public void load(Serializable ... obj);
    
    /**
     * clear the status of this cachedOp
     */
    public void clear();
    
    /**
     * this is a dispose method (should be called when this object is no more used)
     */
    public void dispose();

    /**
     * @param key
     * @param o
     * @return the cached object
     * @throws IOException
     */
    public T getCache(C... o) throws IOException;

    /**
     * perform a cache update
     * 
     * @param arg the object to cache
     * @return true if success
     * @throws IOException
     */
    public boolean putCache(T... arg) throws IOException;

    /**
     * @return true if cache was already called and cached value is not dirty
     * @throws IOException
     */
    public boolean isCached(K... o) throws IOException;

    /**
     * Should be used to set cached objects or invalidate them
     * 
     * @param key
     * @param isCached
     * @throws IOException
     */
    public void setCached(boolean isCached, K... key) throws IOException;

}
