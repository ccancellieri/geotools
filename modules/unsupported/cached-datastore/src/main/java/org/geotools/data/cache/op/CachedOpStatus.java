package org.geotools.data.cache.op;

import java.io.IOException;

public interface CachedOpStatus<K> {

    /**
     * Is this status applicable for passed operation
     * @param op
     * @return
     */
    public boolean isApplicable(Operation op);
    
    /**
     * clear the status of this cachedOp
     * 
     * @throws IOException
     */
    public void clear() throws IOException;

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

    public boolean isDirty(K key) throws IOException;

    void setDirty(K key, boolean value) throws IOException;
    
}
