package org.geotools.data.cache.op;

import java.io.IOException;
import java.util.Calendar;

import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.simple.SimpleSchema;
import org.geotools.feature.type.AttributeDescriptorImpl;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

public class SchemaOp extends BaseOp<SimpleFeatureType, Name, Name> {

    // final Map<Name,SimpleFeatureType> schemas=new HashMap<Name, SimpleFeatureType>();

    public SchemaOp(CacheManager cacheManager, final String uniqueName) {
        super(cacheManager, uniqueName);
    }

    @Override
    public boolean putCache(SimpleFeatureType... schema) throws IOException {
        
        verify(schema);

        final SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();

        for (SimpleFeatureType s : schema) {

            Name name=s.getName();
            // initialize the builder
            b.init(s);

            // add TIMESTAMP
            b.add(new AttributeDescriptorImpl(SimpleSchema.DATETIME, new NameImpl("TimeStamp"), 1,
                    1, false, Calendar.getInstance().getTime()));

            // cache hits
            b.add(new AttributeDescriptorImpl(SimpleSchema.LONG, new NameImpl("Hits"), 1, 1, false,
                    0));

            if (isCached(name)){
                // cache.createSchema(SimpleFeatureTypeBuilder.copy(store.getSchema(name)));
                cache.updateSchema(name,b.buildFeatureType());
            } else {
                cache.createSchema(b.buildFeatureType());
            }
            
            // set cached true
            setCached(Boolean.TRUE,name);
        }
        return Boolean.TRUE;
    }

    @Override
    public SimpleFeatureType getCache(Name... o) throws IOException {
        verify(o);
        return cache.getSchema(o[0]);
    }

}
