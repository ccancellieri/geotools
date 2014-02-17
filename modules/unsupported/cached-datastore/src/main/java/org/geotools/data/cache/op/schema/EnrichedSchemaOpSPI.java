package org.geotools.data.cache.op.schema;

import java.io.IOException;
import java.io.Serializable;

import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.BaseOpStatus;
import org.geotools.data.cache.op.CachedOpStatus;
import org.geotools.data.cache.op.Operation;
import org.opengis.feature.type.Name;
import org.springframework.stereotype.Component;

@Component
public class EnrichedSchemaOpSPI extends SchemaOpSPI {

    /** serialVersionUID */
    private static final long serialVersionUID = 138606816493435171L;

    @Override
    public Operation getOp() {
        return Operation.schema;
    }

    @Override
    public SchemaOp createInstance(CacheManager cacheManager, final BaseOpStatus<Name> status)
            throws IOException {
        return new EnrichedSchemaOp(cacheManager, status);
    }

}
