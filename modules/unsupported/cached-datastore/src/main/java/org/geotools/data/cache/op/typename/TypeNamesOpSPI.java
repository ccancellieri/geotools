package org.geotools.data.cache.op.typename;

import java.io.IOException;
import java.util.List;

import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.BaseOpSPI;
import org.geotools.data.cache.op.Operation;
import org.opengis.feature.type.Name;
import org.springframework.stereotype.Component;

@Component
public class TypeNamesOpSPI extends BaseOpSPI<TypeNameOpStatus, TypeNamesOp, String, List<Name>> {

    /** serialVersionUID */
    private static final long serialVersionUID = 8002106793427576653L;

    @Override
    public Operation getOp() {
        return Operation.typeNames;
    }

    @Override
    public TypeNamesOp createInstance(CacheManager cacheManager, final TypeNameOpStatus status)
            throws IOException {
        return new TypeNamesOp(cacheManager, status);
    }

    @Override
    public TypeNameOpStatus createStatus() {
        return new TypeNameOpStatus();
    }
}
