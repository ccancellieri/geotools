package org.geotools.data.cache.op.schema;

import java.io.IOException;
import java.io.Serializable;

import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.BaseOpStatus;
import org.geotools.data.cache.op.Operation;
import org.opengis.feature.type.Name;

public class SchemaOpStatus extends BaseOpStatus<Name> implements Serializable {

    /** serialVersionUID */
    private static final long serialVersionUID = 1L;

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
