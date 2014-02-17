package org.geotools.data.cache.op.feature;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.BaseOp;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public abstract class BaseFeatureOp<T> extends BaseOp<BaseFeatureOpStatus, Query, T> {
    protected final transient Logger LOGGER = org.geotools.util.logging.Logging
            .getLogger(getClass().getPackage().getName());

    public BaseFeatureOp(CacheManager cacheManager, final BaseFeatureOpStatus status)
            throws IOException {
        super(cacheManager, status);
    }

    /**
     * Override this method to clear the features into the cached feature source <br/>
     * NOTE: in the overriding method remember to call super.clear().
     */
    @Override
    public void clear() throws IOException {
        if (status == null) {
            throw new IllegalStateException("Status is null");
        }
        status.clear();

        // if on this instance has been set the entry we may have written some features, let's remove them
        if (status.getEntry() != null) {
            FeatureWriter<SimpleFeatureType, SimpleFeature> fw = null;
            try {
                fw = cacheManager.getCache().getFeatureWriter(status.getEntry().getTypeName(),
                        Transaction.AUTO_COMMIT);
                while (fw.hasNext()) {
                    fw.next();
                    fw.remove();
                }
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, e.getLocalizedMessage(), e);
            } finally {
                if (fw != null) {
                    try {
                        fw.close();
                    } catch (IOException e) {
                        LOGGER.log(Level.WARNING, e.getLocalizedMessage(), e);
                    }
                }
            }
        }
        super.clear();
    }
    
    
    /**
     * @return delegate to the status
     */
    public boolean isFullyCached() {
        return status.isFullyCached();
    }

}
