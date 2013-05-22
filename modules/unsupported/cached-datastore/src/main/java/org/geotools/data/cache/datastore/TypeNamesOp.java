package org.geotools.data.cache.datastore;

import java.io.IOException;
import java.util.List;

import org.geotools.data.DataStore;
import org.geotools.data.cache.op.BaseOp;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.type.Name;

public class TypeNamesOp extends BaseOp<List<Name>> {

    public TypeNamesOp(DataStore ds, DataStore cds) {
        super(ds, cds);
    }

    @Override
    protected List<Name> getCachedInternal() throws IOException {
        return cache.getNames();
    }

    @Override
    public boolean cache(List<Name> arg) throws IOException {
        // create schemas
        for (Name name : arg) {
            cache.createSchema(SimpleFeatureTypeBuilder.copy(store.getSchema(name)));
        }
        return true;
    }

    @Override
    public List<Name> operation() throws IOException {
        return store.getNames();
    }
}
