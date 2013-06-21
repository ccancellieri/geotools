package org.geotools.data.cache.op;

import java.io.IOException;
import java.util.List;

import org.opengis.feature.type.Name;

public class TypeNamesOp extends BaseOp<List<Name>, TypeNamesOp, TypeNamesOp> {

    public TypeNamesOp(CacheManager cacheManager, final String uniqueName) {
        super(cacheManager,uniqueName);
    }

    @Override
    public List<Name> getCache(TypeNamesOp... o) throws IOException {
        return cache.getNames();
    }

    @Override
    public boolean isCached(TypeNamesOp... o) {
        return super.isCached(this);
    };

    @Override
    public void setCached(boolean isCached, TypeNamesOp... key) {
        super.setCached(true, this);
    };

    @Override
    public boolean putCache(List<Name>... arg) throws IOException {
        verify(arg);
        final SchemaOp op = (SchemaOp) cacheManager.getCachedOp(Operation.schema);
        // create schemas
        for (Name name : arg[0]) {
            if (op != null) {
                // if (op.isCached(name)) {
                // force update
                op.putCache(source.getSchema(name));
                // cache.updateSchema(name, op.getCache(name));
                // } else {
                // update schemaOp cache
                // op.putCache(source.getSchema(name));
                // actually cache the schema caching it into the TypeNameOp
                // cache.createSchema(op.getCache(name));
                // }
            } else {
                // LOG warn no schema wrapping means no timestamp and no hitcache
                cache.createSchema(source.getSchema(name));
            }

        }
        // set cached true
        setCached(Boolean.TRUE, this);

        return Boolean.TRUE;
    }
}
