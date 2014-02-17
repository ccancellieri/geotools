package org.geotools.data.cache.op.typename;

import java.io.IOException;
import java.util.List;

import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.BaseOp;
import org.opengis.feature.type.Name;

public class TypeNamesOp extends BaseOp<TypeNameOpStatus, String, List<Name>> {
    
    public TypeNamesOp(CacheManager cacheManager, final TypeNameOpStatus status)
            throws IOException {
        super(cacheManager, status);
    }

    @Override
    public List<Name> getCache(String o) throws IOException {
        return getStatus().getNames();
    }

    /**
     * @param arguments are unused
     */
    @Override
    public List<Name> updateCache(String arg) throws IOException {
        final List<Name> names = cacheManager.getSource().getNames();
        List<Name> statusNames=getStatus().getNames();
        statusNames.addAll(names);
        setCached(arg, true);
        return statusNames;
    }
    
//    /**
//     * @param arguments are unused
//     */
//    @Override
//    public List<Name> updateCache(String arg) throws IOException {
//        final List<Name> names = cacheManager.getSource().getNames();
//        final SchemaOp schemaOp = cacheManager.getCachedOpOfType(Operation.schema, SchemaOp.class);
//        if (schemaOp != null) {
//            // create schemas
//            for (Name name : names) {
//                SimpleFeatureType schema = null;
//                if (!schemaOp.isCached(name) || schemaOp.isDirty(name)) {
//                    schema = schemaOp.updateCache(name);
//                    schemaOp.setCached(name, schema != null ? true : false);
//                } else {
//                    schema = schemaOp.getCache(name);
//                }
//            }
//        }
//        return names;
//    }

}
