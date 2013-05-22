package org.geotools.data.cache.op;

import java.io.IOException;

public interface CachedOp<T> {

    /**
     * @return the cached object
     * @throws IOException
     */
    public T getCached() throws IOException;
    
    /**
     * The call (operation) to cache
     * @return the object to cache
     * @throws IOException
     */
    public T operation() throws IOException;

    /**
     * perform a cache update
     * @param arg the object to cache
     * @return true if success
     * @throws IOException
     */
    public boolean cache(T arg) throws IOException;


}
