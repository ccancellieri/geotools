package org.geotools.data.cache.op;

import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentEntry;

public abstract class BaseFeatureSourceOp extends BaseOp<SimpleFeatureSource,String,String> {

//    protected String typeName;
    
    protected ContentEntry entry;

    public ContentEntry getEntry() {
        return entry;
    }

    public void setEntry(ContentEntry entry) {
        this.entry = entry;
    }

    public BaseFeatureSourceOp(CacheManager cacheManager, final String uniqueName) {//, String typeName
        super(cacheManager, uniqueName);
        
//        this.typeName = typeName;
    }

//    public String getTypeName() {
//        return typeName;
//    }
//
//    public void setTypeName(String typeName) {
//        this.typeName = typeName;
//    }

//    @Override
//    public SimpleFeatureSource getCached(String o) throws IOException {
//        SimpleFeatureSource op=null;
//        if (!isCached){
//            op=operation(o);
//            isCached=cache(op);
//        }
//        if (isCached){
//            return cache.getFeatureSource(o);
//        } else {
//            return op;
//        }
//    }

    // @Override
    // public SimpleFeatureSource operation(String o) throws IOException {
    // return source.getFeatureSource(o);
    // }
    
}
