package org.geotools.data.cache.op;

import java.io.IOException;

import org.springframework.stereotype.Service;

@Service
public class CountOpSPI extends CachedOpSPI<CountOp> {

    /** serialVersionUID */
    private static final long serialVersionUID = 8822095832437602003L;

    @Override
    public Operation getOp() {
        return Operation.count;
    }

    @Override
    protected CountOp createInstance(CacheManager cacheManager, final String uniqueName)
            throws IOException {
        return new CountOp(cacheManager, uniqueName);
    }

    @Override
    public long priority() {
        return 0;
    }
}
