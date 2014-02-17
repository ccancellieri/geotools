package org.geotools.data.cache.op.schema;

import java.io.IOException;
import java.io.Serializable;

import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.BaseOpSPI;
import org.geotools.data.cache.op.BaseOpStatus;
import org.geotools.data.cache.op.Operation;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.springframework.stereotype.Component;

@Component
public class SchemaOpSPI extends BaseOpSPI<BaseOpStatus<Name>, SchemaOp, Name, SimpleFeatureType>
        implements Serializable {

    /** serialVersionUID */
    private static final long serialVersionUID = 138606816493435171L;

    @Override
    public Operation getOp() {
        return Operation.schema;
    }

    @Override
    public SchemaOp createInstance(CacheManager cacheManager, final BaseOpStatus<Name> uniqueName)
            throws IOException {
        return new SchemaOp(cacheManager, uniqueName);
    }
    

}
