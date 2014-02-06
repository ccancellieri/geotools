package org.geotools.data.cache.op.schema;

import java.io.IOException;

import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.BaseOpStatus;
import org.geotools.data.cache.op.Operation;
import org.opengis.feature.type.Name;

public class SchemaOpStatus extends BaseOpStatus<Name> {


    public SchemaOpStatus(CacheManager cacheManager, final BaseOpStatus<Name> status)
            throws IOException {
        // super(cacheManager, status);
    }

    @Override
    public boolean isApplicable(Operation op) {
        if (op.equals(Operation.schema)) {
            return true;
        }
        return false;
    }

}
