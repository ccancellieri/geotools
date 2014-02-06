package org.geotools.data.cache.op.count;

import java.io.IOException;

import org.geotools.data.Query;
import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.BaseOpSPI;
import org.geotools.data.cache.op.CachedOpStatus;
import org.geotools.data.cache.op.Operation;
import org.springframework.stereotype.Service;

@Service
public class CountOpSPI extends BaseOpSPI<CountOp,Query,Integer> {

    /** serialVersionUID */
    private static final long serialVersionUID = 8822095832437602003L;

    @Override
    public Operation getOp() {
        return Operation.count;
    }

    @Override
    public CountOp createInstance(CacheManager cacheManager, final CachedOpStatus<Query> status)
            throws IOException {
        return new CountOp(cacheManager, status);
    }
}
