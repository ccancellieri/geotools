package org.geotools.data.cache.op;

import java.io.IOException;

import org.springframework.stereotype.Component;

@Component
public class TypeNamesOpSPI extends CachedOpSPI<TypeNamesOp> {

    /** serialVersionUID */
    private static final long serialVersionUID = 8002106793427576653L;

    @Override
    public Operation getOp() {
        return Operation.typeNames;
    }

    @Override
    protected TypeNamesOp createInstance(CacheManager cacheManager, final String uniqueName) throws IOException {
        return new TypeNamesOp(cacheManager,uniqueName);
    }

    @Override
    public long priority() {
        return 0;
    }
}
