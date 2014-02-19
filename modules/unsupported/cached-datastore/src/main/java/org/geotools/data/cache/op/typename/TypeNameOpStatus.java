package org.geotools.data.cache.op.typename;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.geotools.data.cache.op.BaseOpStatus;
import org.geotools.data.cache.op.Operation;
import org.opengis.feature.type.Name;

public class TypeNameOpStatus extends BaseOpStatus<String> implements Serializable {

    /** serialVersionUID */
    private static final long serialVersionUID = 1L;
    
    private final ArrayList<Name> names;

    public TypeNameOpStatus() {
        names = new ArrayList<Name>();
    }

    public List<Name> getNames() {
        return names;
    }

    @Override
    public boolean isApplicable(Operation op) {
        if (op.equals(Operation.typeNames)) {
            return true;
        }
        return false;
    }

}
