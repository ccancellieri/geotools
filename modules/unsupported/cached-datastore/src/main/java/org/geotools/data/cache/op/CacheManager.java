package org.geotools.data.cache.op;

import java.io.IOException;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.DataStore;

public class CacheManager {
    private final transient Logger LOGGER = org.geotools.util.logging.Logging.getLogger(getClass()
            .getPackage().getName());

    // datastore
    private final transient DataStore source;

    // cache
    private final transient DataStore cache;

    private final CacheStatus status;

    private final String uid;

    public CacheManager(DataStore store, DataStore cache, final String uid) {
        if (store == null || cache == null)
            throw new IllegalArgumentException(
                    "Unable to create the cache manager with null source or cache datastore");
        this.source = store;
        this.cache = cache;
        this.uid = uid;
        // create a status configuration
        status = new CacheStatus(uid);
    }

    public String getUID() {
        return uid;
    }

    public CacheStatus getStatus() {
        return status;
    }

    /**
     * Delegate to {@link CacheStatus#getCachedOp(CachedOpSPI)}
     * 
     * @param op
     * @return
     */
    public CachedOp<?, ?, ?> getCachedOp(Operation op) {
        return status.getCachedOp(op);
    }
    
    public <T> T getCachedOpOfType(Operation op, Class<T> clazz) {
        final Object obj=status.getCachedOp(op);
        if (obj==null)
            return null;
        if (clazz.isAssignableFrom(obj.getClass()))
            return clazz.cast(obj);
        else
            return null;
    }
    

    public void save() {
        status.save();
    }

    /**
     * loads from the cache the status of this {@link CacheManager} creating a new set of CachedOpSPI using the cached SPI set
     * 
     * @param uniqueName
     */
    public void load() {
        status.load(this);
    }

    /**
     * loads from the cache the status of this {@link CacheManager} creating a new set of CachedOpSPI using the passed SPI set
     * 
     * @param uniqueName
     */
    public void load(final Collection<CachedOpSPI<CachedOp<?, ?, ?>>> spiList) {
        for (CachedOpSPI<CachedOp<?, ?, ?>> spi : spiList) {
            try {
                status.loadOp(this, spi);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, e.getMessage(), e);
            }
        }
    }

    public DataStore getSource() {
        return source;
    }

    public DataStore getCache() {
        return cache;
    }

    public void dispose() {
        if (status != null) {
            status.dispose();
        }
        if (source != null)
            source.dispose();
        if (cache != null)
            cache.dispose();
    }

    // public String createCachedOpUID(Operation op) {
    // return new StringBuilder(status.getUID()).append(op.toString()).toString();
    // }
}
