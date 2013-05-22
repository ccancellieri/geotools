package org.geotools.data.cache.op;

import java.io.IOException;

import org.geotools.data.DataStore;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentEntry;

public abstract class BaseFeatureSourceOp extends BaseOp<SimpleFeatureSource> {

    protected String typeName;
    
    protected ContentEntry entry;

    public ContentEntry getEntry() {
        return entry;
    }

    public void setEntry(ContentEntry entry) {
        this.entry = entry;
    }

    public BaseFeatureSourceOp(DataStore ds, DataStore cds, String typeName) {
        super(ds, cds);
        
        this.typeName = typeName;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    @Override
    public SimpleFeatureSource getCached() throws IOException {
        SimpleFeatureSource op=null;
        if (!isCached){
            op=operation();
            isCached=cache(op);
        }
        if (isCached){
            return cache.getFeatureSource(typeName);
        } else {
            return op;
        }
    }

    @Override
    public SimpleFeatureSource operation() throws IOException {
        return store.getFeatureSource(typeName);
    }
    
}
