package org.geotools.data.cache.op;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;

import org.geotools.data.DataStore;
import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.simple.SimpleSchema;
import org.geotools.feature.type.AttributeDescriptorImpl;
import org.opengis.feature.type.Name;

public class TypeNamesOp extends BaseOp<List<Name>> {

    public TypeNamesOp(DataStore ds, DataStore cds, CacheManager cacheManager) {
        super(ds, cds, cacheManager);
    }

    @Override
    protected List<Name> getCachedInternal() throws IOException {
        return cache.getNames();
    }

    @Override
    public boolean cache(List<Name> arg) throws IOException {
        // create schemas
        for (Name name : arg) {
            final SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();

            // initialize the builder
            b.init(store.getSchema(name));

            // add TIMESTAMP
            b.add(new AttributeDescriptorImpl(SimpleSchema.DATETIME, new NameImpl("TimeStamp"), 1,
                    1, false, Calendar.getInstance().getTime()));

            // cache hits
            b.add(new AttributeDescriptorImpl(SimpleSchema.LONG, new NameImpl("Hits"), 1, 1, false,
                    0));

            cache.createSchema(b.buildFeatureType());
            // cache.createSchema(SimpleFeatureTypeBuilder.copy(store.getSchema(name)));
        }
        return true;
    }

    @Override
    public List<Name> operation() throws IOException {
        return store.getNames();
    }
}
