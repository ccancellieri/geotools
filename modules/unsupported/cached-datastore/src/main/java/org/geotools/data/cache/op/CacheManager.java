package org.geotools.data.cache.op;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.DataStore;
import org.geotools.data.cache.utils.CacheUtils;

public class CacheManager {
    private final Logger LOGGER = org.geotools.util.logging.Logging.getLogger(getClass()
            .getPackage().getName());

    // datastore
    private final DataStore store;

    // cache
    private final DataStore cache;

    // operations
    private Map<Operation, CachedOp<?>> cacheMap;

    public CacheManager(DataStore store, DataStore cache) {
        this.store = store;
        this.cache = cache;
    }

    public CachedOp<?> getCachedOp(Operation op) {
        return getCacheMap().get(op);
    }

    public void putCachedOp(Operation op, CachedOp<?> cachedOp) {
        getCacheMap().put(op, cachedOp);
    }

    private Map<Operation, CachedOp<?>> getCacheMap() {
        if (this.cacheMap == null) {
            synchronized (this) {
                if (this.cacheMap == null) {
                    final CacheUtils cu = CacheUtils.getCacheUtils();
                    if (cu != null) {
                        for (Operation op : Operation.values()) {
                            for (CachedOpSPI<?> spi : cu.getCachedOps()) {
                                if (spi.getOp().equals(op)) {
                                    try {
                                        putCachedOp(op, spi.create(store, cache, this));
                                    } catch (IOException e) {
                                        LOGGER.log(Level.SEVERE, e.getMessage(), e);
                                    }
                                }
                            }
                        }
                    } else {
                        if (LOGGER.isLoggable(Level.SEVERE)) {
                            LOGGER.log(Level.SEVERE, "Unable to load " + CacheUtils.class);
                        }
                        return new HashMap<Operation, CachedOp<?>>();
                    }
                }
            }
        }
        return cacheMap;
    }

}
