package org.geotools.data.cache.op.next;

import java.io.IOException;

import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.BaseOpStatus;
import org.geotools.data.cache.op.Operation;
import org.opengis.feature.simple.SimpleFeature;
import org.springframework.stereotype.Component;

@Component
public class EnrichedNextOpSPI extends NextOpSPI {

    /** serialVersionUID */
    private static final long serialVersionUID = 138606816493435171L;

    @Override
    public Operation getOp() {
        return Operation.next;
    }

    @Override
    public NextOp createInstance(CacheManager cacheManager, final BaseOpStatus<SimpleFeature> status)
            throws IOException {
        return new EnrichedNextOp(cacheManager, status);
    }

}
