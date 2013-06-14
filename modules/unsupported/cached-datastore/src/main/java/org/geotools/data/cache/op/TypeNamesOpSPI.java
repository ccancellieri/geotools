package org.geotools.data.cache.op;

import java.io.IOException;

import org.geotools.data.DataStore;
import org.springframework.stereotype.Component;

@Component
public class TypeNamesOpSPI extends CachedOpSPI<TypeNamesOp> {

    @Override
    public Operation getOp() {
        return Operation.typeNames;
    }

    @Override
    protected TypeNamesOp createInstance(DataStore source, DataStore cache,
            CacheManager cacheManager) throws IOException {
        return new TypeNamesOp(source, cache, cacheManager);
    }

    @Override
    public long priority() {
        return 0;
    }
}
