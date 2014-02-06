package org.geotools.data.cache.op.next;

import java.io.IOException;
import java.io.Serializable;

import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.CachedOpStatus;
import org.geotools.data.cache.op.Operation;
import org.opengis.feature.simple.SimpleFeature;
import org.springframework.stereotype.Component;

@Component
public class EnrichedNextOpSPI extends NextOpSPI implements Serializable {

    /** serialVersionUID */
    private static final long serialVersionUID = 138606816493435171L;

    @Override
    public Operation getOp() {
        return Operation.next;
    }

    @Override
    public NextOp createInstance(CacheManager cacheManager, final CachedOpStatus<SimpleFeature> status) throws IOException {
        return new EnrichedNextOp(cacheManager, status);
    }

    @Override
    public long priority() {
        return 0;
    }
}
