package org.geotools.data.cache.op;

import java.io.IOException;
import java.io.Serializable;

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
    protected NextOp createInstance(CacheManager cacheManager, final String uniqueName)
            throws IOException {
        return new EnrichedNextOp(cacheManager, uniqueName);
    }

    @Override
    public long priority() {
        return 0;
    }
}
