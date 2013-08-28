package org.geotools.data.cache.op;

import org.geotools.data.store.ContentEntry;

public abstract class BaseFeatureSourceOp<T, K, C> extends BaseOp<T, K, C> {

    private ContentEntry entry;

    public ContentEntry getEntry() {
        return entry;
    }

    public void setEntry(ContentEntry entry) {
        this.entry = entry;
    }

    public BaseFeatureSourceOp(CacheManager cacheManager, final String uid) {
        super(cacheManager, uid);
    }

}
