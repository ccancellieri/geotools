package org.geotools.data.cache.datastore;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.geotools.data.cache.op.CachedOpSPI;
import org.geotools.data.cache.op.CachedOpStatus;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.cache.op.feature.BaseFeatureOpStatus;

/**
 * 
 * @author Carlo Cancellieri - GeoSolutions SAS
 * 
 */
public class CacheStatus implements Serializable {
    // private final transient Logger LOGGER = org.geotools.util.logging.Logging.getLogger(getClass()
    // .getPackage().getName());

    /** serialVersionUID */
    private static final long serialVersionUID = 1L;

    // name for the operations map storage
    private final String typeName;

    // operations
    private Map<String, CachedOpSPI<CachedOpStatus<?>, ?, ?, ?>> cachedOpSPIMap;

    // status
    private final Map<String, CachedOpStatus<?>> cachedOpStatusMap = new HashMap<String, CachedOpStatus<?>>();

    private final ReadWriteLock statusLock = new ReentrantReadWriteLock();

    public CacheStatus(final String typeName,
            Map<String, CachedOpSPI<CachedOpStatus<?>, ?, ?, ?>> cachedOpSPIMap) {
        this(typeName, cachedOpSPIMap, true);
    }
    
    public CacheStatus(final String typeName,
            Map<String, CachedOpSPI<CachedOpStatus<?>, ?, ?, ?>> cachedOpSPIMap, final boolean sharedFeatureStatus) {
        if (typeName == null || typeName.isEmpty())
            throw new IllegalArgumentException(
                    "Unable to create the cache status with null or empty uid");

        this.typeName = typeName;

        if (cachedOpSPIMap == null) {
            this.cachedOpSPIMap = new HashMap<String, CachedOpSPI<CachedOpStatus<?>, ?, ?, ?>>();
        } else {
            // initialize the map
            this.cachedOpSPIMap = cachedOpSPIMap;// setCachedOpSPI(new ArrayList<CachedOpSPI<CachedOpStatus<?>,?,?,?>>(cachedOpSPIMap.values()));
            BaseFeatureOpStatus featureStatus = null;
            if (sharedFeatureStatus){
                // building related status
                for (CachedOpSPI<CachedOpStatus<?>, ?, ?, ?> spi : cachedOpSPIMap.values()) {
                    // store created status
                    
                    if (featureStatus!=null && featureStatus.isApplicable(spi.getOp())){
                        putCachedOpStatus(spi.getOp().toString(), featureStatus);
                        continue;
                    }
                    
                    final CachedOpStatus<?> status = spi.createStatus();
                    
                    if (BaseFeatureOpStatus.class.isAssignableFrom(status.getClass())){
                        featureStatus=BaseFeatureOpStatus.class.cast(status);
                    }
                    
                    putCachedOpStatus(spi.getOp().toString(), status);
                }
            } else {
             // building related status
                for (CachedOpSPI<CachedOpStatus<?>, ?, ?, ?> spi : cachedOpSPIMap.values()) {
                    // store created status
                    
                    final CachedOpStatus<?> status = spi.createStatus();
                    
                    putCachedOpStatus(spi.getOp().toString(), status);
                }
            }
        }
    }

    public String getUID() {
        return this.typeName;
    }

    void clear() throws IOException {
        try {
            statusLock.writeLock().lock();
            cachedOpSPIMap.clear();
        } finally {
            statusLock.writeLock().unlock();
        }

    }

    Set<String> getCachedOpKeys() {
        try {
            this.statusLock.readLock().lock();
            return cachedOpSPIMap.keySet();
        } finally {
            this.statusLock.readLock().unlock();
        }
    }

    Collection<CachedOpSPI<CachedOpStatus<?>, ?, ?, ?>> getCachedOps() {
        try {
            this.statusLock.readLock().lock();
            return cachedOpSPIMap.values();
        } finally {
            this.statusLock.readLock().unlock();
        }
    }

    CachedOpSPI<?, ?, ?, ?> getCachedOpSPI(CachedOpSPI<?, ?, ?, ?> op) {
        try {
            this.statusLock.readLock().lock();
            return cachedOpSPIMap.get(op.toString());
        } finally {
            this.statusLock.readLock().unlock();
        }
    }

    CachedOpStatus<?> getCachedOpStatus(Operation op) {
        try {
            this.statusLock.readLock().lock();
            return cachedOpStatusMap.get(op.toString());
        } finally {
            this.statusLock.readLock().unlock();
        }
    }

    <T extends CachedOpStatus<?>> T getCachedOpStatusOfType(Operation op) {
        try {
            this.statusLock.readLock().lock();
            return (T) cachedOpStatusMap.get(op.toString());
        } finally {
            this.statusLock.readLock().unlock();
        }
    }

    CachedOpStatus<?> putCachedOpStatus(String op, CachedOpStatus<?> cachedOpStatus) {
        try {
            this.statusLock.writeLock().lock();
            return cachedOpStatusMap.put(op, cachedOpStatus);
        } finally {
            this.statusLock.writeLock().unlock();
        }
    }

}
