package org.geotools.data.cache.datastore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import org.geotools.data.cache.op.CachedOpSPI;
import org.geotools.data.cache.op.CachedOpStatus;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.cache.op.feature.BaseFeatureOpStatus;

import sun.misc.SharedSecrets;

/**
 * 
 * @author Carlo Cancellieri - GeoSolutions SAS
 * 
 */
public class CacheStatus {
    private final transient Logger LOGGER = org.geotools.util.logging.Logging.getLogger(getClass()
            .getPackage().getName());

    // name for the operations map storage
    private final String uid;

    // operations
    private final Map<Operation, CachedOpSPI<?, ?, ?>> cachedOpSPIMap = new HashMap<Operation, CachedOpSPI<?, ?, ?>>();

    // status
    private final Map<Operation, CachedOpStatus<?>> cachedOpStatusMap = new HashMap<Operation, CachedOpStatus<?>>();

    private final transient ReadWriteLock statusLock = new ReentrantReadWriteLock();

    public CacheStatus(final String uid, Map<Operation, CachedOpSPI<?, ?, ?>> cachedOpSPIMap) {
        if (uid == null || uid.isEmpty())
            throw new IllegalArgumentException(
                    "Unable to create the cache status with null or empty uid");

        this.uid = uid;

    }

    public String getUID() {
        return this.uid;
    }

    void clear() throws IOException {
        try {
            statusLock.writeLock().lock();
            cachedOpSPIMap.clear();
        } finally {
            statusLock.writeLock().unlock();
        }

    }

    Set<Operation> getCachedOpKeys() {
        try {
            this.statusLock.readLock().lock();
            return cachedOpSPIMap.keySet();
        } finally {
            this.statusLock.readLock().unlock();
        }
    }

    Collection<CachedOpSPI<?, ?, ?>> getCachedOps() {
        try {
            this.statusLock.readLock().lock();
            return cachedOpSPIMap.values();
        } finally {
            this.statusLock.readLock().unlock();
        }
    }

    /**
     * set the new spi collection comparing the stored set of SPI with the passed one and returning a collection of operation which are changed (this
     * should be used to clear the matching cachedOp before a new one is created into the CacheManager)
     * 
     * @param spiColl
     * @return
     */
    Collection<Operation> setCachedOpSPI(Collection<CachedOpSPI<?, ?, ?>> spiColl) {
        final boolean sharedFeatureStatus = true;
        BaseFeatureOpStatus featureStatus = null;
        boolean clearCheckLoop=false;

        final List<Operation> returns = new ArrayList<Operation>();
        try {
            this.statusLock.writeLock().lock();

            // for each selected SPI changes
            final Iterator<CachedOpSPI<?, ?, ?>> it = spiColl.iterator();
            while (it.hasNext()) {
                final CachedOpSPI<?, ?, ?> selectedOpSPI = it.next();
                // check stored to compare with selected SPI
                if (cachedOpSPIMap.containsKey(selectedOpSPI.getOp())) {
                    final CachedOpSPI<?, ?, ?> storedOpSPI = cachedOpSPIMap.get(selectedOpSPI
                            .getOp());
                    if (selectedOpSPI.getOp().equals(storedOpSPI.getOp())) {
                        // if stored is equals with the selected no change is required
                        if (!selectedOpSPI.equals(storedOpSPI)) {
                            // substitute the the stored operation with the new one
                            cachedOpSPIMap.put(selectedOpSPI.getOp(), selectedOpSPI);
                            if (sharedFeatureStatus && featureStatus != null
                                    && featureStatus.isApplicable(selectedOpSPI.getOp())) {
                                cachedOpStatusMap.put(selectedOpSPI.getOp(), featureStatus);
                            } else {
                                CachedOpStatus<?> status = selectedOpSPI.createStatus();
                                if (status != null
                                        && BaseFeatureOpStatus.class.isAssignableFrom(status
                                                .getClass())) {
                                    featureStatus = (BaseFeatureOpStatus) status;
                                    cachedOpStatusMap.put(selectedOpSPI.getOp(), featureStatus);
                                } else {
                                    cachedOpStatusMap.put(selectedOpSPI.getOp(), status);
                                }
                            }

                            // add the operation to the return list
                            returns.add(storedOpSPI.getOp());
                        }
                    }
                } else {
                    // the selected SPI is not present into the store map, let's add it
                    cachedOpSPIMap.put(selectedOpSPI.getOp(), selectedOpSPI);
                    cachedOpStatusMap.put(selectedOpSPI.getOp(), selectedOpSPI.createStatus());
                }
            }

            // now some SPI into the cachedOpSPIMap may be absent into the selection (unselected)
            // lets remove them from the stored map adding them to the returns list.
            final Iterator<Entry<Operation, CachedOpSPI<?, ?, ?>>> itSpi = cachedOpSPIMap
                    .entrySet().iterator();
            final Iterator<Entry<Operation, CachedOpStatus<?>>> itStatus = cachedOpStatusMap
                    .entrySet().iterator();
            while (itSpi.hasNext()) {
                final Entry<Operation, CachedOpSPI<?, ?, ?>> entry = itSpi.next();
                final Entry<Operation, CachedOpStatus<?>> status = itStatus.next();

                final Operation op = entry.getKey();
                // if the selected operationSPI collection does not contains some SPI in the stored map
                // boolean found = false;
                final Iterator<CachedOpSPI<?, ?, ?>> it4 = spiColl.iterator();
                while (it4.hasNext()) {
                    final CachedOpSPI<?, ?, ?> selectedOpSPI = it4.next();
                    if (selectedOpSPI.getOp().equals(op)) {
                        // add to the change list
                        returns.add(op);
                        try {
                            if (!sharedFeatureStatus){
                                status.getValue().clear();
                            } else if (featureStatus!=null && status.equals(featureStatus)){
                                // see below for cache memory leak clear loop
                                clearCheckLoop=true;
                            }
                        } catch (IOException e) {
                            LOGGER.severe(e.getMessage());
                        }
                        // remove from the store
                        itSpi.remove();
                        itStatus.remove();
                    }
                }
            }
        } finally {
            this.statusLock.writeLock().unlock();
        }
        
        // cache memory leak clear loop
        if (clearCheckLoop){
            boolean found=false;
            final Iterator<Entry<Operation, CachedOpStatus<?>>> itStatus = cachedOpStatusMap
                    .entrySet().iterator();
            while (itStatus.hasNext()) {
                final Entry<Operation, CachedOpStatus<?>> status = itStatus.next();
                if (status.equals(featureStatus)){
                    found=true;
                }
            }
            if (!found){
                try {
                    featureStatus.clear();
                } catch (IOException e) {
                    LOGGER.severe(e.getMessage());
                }
            }
        }
        return returns;
    }

    CachedOpSPI<?, ?, ?> getCachedOpSPI(CachedOpSPI<?, ?, ?> op) {
        try {
            this.statusLock.readLock().lock();
            return cachedOpSPIMap.get(op);
        } finally {
            this.statusLock.readLock().unlock();
        }
    }

    CachedOpStatus<?> getCachedOpStatus(Operation op) {
        try {
            this.statusLock.readLock().lock();
            return cachedOpStatusMap.get(op);
        } finally {
            this.statusLock.readLock().unlock();
        }
    }

    CachedOpStatus<?> putCachedOpStatus(Operation op, CachedOpStatus<?> cachedOpStatus) {
        try {
            this.statusLock.writeLock().lock();
            return cachedOpStatusMap.put(op, cachedOpStatus);
        } finally {
            this.statusLock.writeLock().unlock();
        }
    }

}
