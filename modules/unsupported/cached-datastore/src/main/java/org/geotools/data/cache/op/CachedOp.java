package org.geotools.data.cache.op;

import java.io.IOException;

/**
 * 
 * @author carlo
 * 
 * @param <T> The type returned by the cachedOp(eration)
 * @param <K> the type used to check if the operation has cached a specific call
 */
public interface  CachedOp<K, T> {


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

}
