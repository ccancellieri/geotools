package org.geotools.data.cache.op;

import java.io.IOException;
import java.util.List;

import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

public class TypeNamesOp extends BaseOp<List<Name>, String, String> {

    public TypeNamesOp(CacheManager cacheManager, final String uniqueName) {
        super(cacheManager, uniqueName);
    }

    @Override
    public List<Name> getCache(String o) throws IOException {
        return cacheManager.getCache().getNames();
    }

    @Override
    public boolean isCached(String o) {
        return super.isCached(getUid()+getClass());
    };

    /**
     * @param isCached - used to set or invalidate cached values
     * @param key - unused
     */
    @Override
    public void setCached(boolean isCached, String key) {
        super.setCached(true, getUid()+getClass());
    };

    /**
     * @param arguments are unused
     */
    @Override
    public List<Name> updateCache(String arg) throws IOException {
        final List<Name> names = cacheManager.getSource().getNames();
        final SchemaOp op = cacheManager.getCachedOpOfType(Operation.schema,SchemaOp.class);
        // create schemas
        for (Name name : names) {
            SimpleFeatureType schema = null;
            if (op != null) {
                if (!op.isCached(cacheManager.getUID())) {
                    schema = op.updateCache(name);
                    op.setCached(schema!=null?true:false, cacheManager.getUID());
                } else {
                    schema = op.getCache(name);
                }
            }
            if (schema != null) {
                cacheManager.getCache().createSchema(schema);
            } else {
                // LOG warn no schema wrapping means no timestamp is used
                cacheManager.getCache().createSchema(cacheManager.getSource().getSchema(name));
            }
        }

        return names;
    }
}
