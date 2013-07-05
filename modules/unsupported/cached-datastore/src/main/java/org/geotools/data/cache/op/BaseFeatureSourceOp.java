package org.geotools.data.cache.op;

import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentEntry;

public abstract class BaseFeatureSourceOp extends BaseOp<SimpleFeatureSource,String,String> {
    
    protected ContentEntry entry;

    public ContentEntry getEntry() {
        return entry;
    }

    public void setEntry(ContentEntry entry) {
        this.entry = entry;
    }

    public BaseFeatureSourceOp(CacheManager cacheManager, final String uniqueName) {
        super(cacheManager, uniqueName);
    }    
}
