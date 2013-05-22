package org.geotools.data.cache.datastore;

import java.io.IOException;

import org.geotools.data.DataStore;
import org.geotools.data.cache.op.CacheOp;
import org.geotools.data.cache.op.CachedOpSPI;
import org.springframework.stereotype.Component;

@Component
public class TypeNamesOpSPI extends CachedOpSPI<TypeNamesOp> {

    @Override
    protected TypeNamesOp createInstance(DataStore source, DataStore cache) throws IOException {
        return new TypeNamesOp(source, cache);
    }

    @Override
    public Object getOp() {
        return CacheOp.typeNames;
    }
}
