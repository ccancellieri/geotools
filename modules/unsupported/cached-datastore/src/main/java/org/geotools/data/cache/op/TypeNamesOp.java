package org.geotools.data.cache.op;

import java.io.IOException;
import java.util.List;

import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

public class TypeNamesOp extends BaseOp<List<Name>, String> {

    public TypeNamesOp(CacheManager cacheManager, final String uniqueName) throws IOException {
        super(cacheManager, uniqueName);
    }

    @Override
    public List<Name> getCache(String o) throws IOException {
        return cacheManager.getCache().getNames();
    }

    /**
     * @param arguments are unused
     */
    @Override
    public List<Name> updateCache(String arg) throws IOException {
        final List<Name> names = cacheManager.getSource().getNames();
        final SchemaOp schemaOp = cacheManager.getCachedOpOfType(Operation.schema, SchemaOp.class);
        if (schemaOp != null) {
            // create schemas
            for (Name name : names) {
                SimpleFeatureType schema = null;
                if (!schemaOp.isCached(name) || schemaOp.isDirty(name)) {
                    schema = schemaOp.updateCache(name);
                    schemaOp.setCached(name, schema != null ? true : false);
                } else {
                    schema = schemaOp.getCache(name);
                }
            }
        }
        return names;
    }

    @Override
    public boolean isDirty(String key) throws IOException {
        return false;
    }

    @Override
    public void setDirty(String query, boolean value) throws IOException {
        throw new UnsupportedOperationException();
    }
}
