package org.geotools.data.cache.op;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Calendar;

import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.simple.SimpleSchema;
import org.geotools.feature.type.AttributeDescriptorImpl;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

public class EnrichedSchemaOp extends SchemaOp {

    public EnrichedSchemaOp(CacheManager cacheManager, final String uniqueName) throws IOException {
        super(cacheManager, uniqueName);
    }

    public static String TIMESTAMP_NAME = "_TimeStamp_";

    public static String HINTS_NAME = "_Hints_";

    @Override
    public SimpleFeatureType updateCache(Name name) throws IOException {

        verify(name);

        final SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();

        final SimpleFeatureType schema = cacheManager.getSource().getSchema(name);

        // initialize the builder
        b.init(schema);

        // add TIMESTAMP
        b.add(new AttributeDescriptorImpl(SimpleSchema.DATETIME, new NameImpl(TIMESTAMP_NAME), 1,
                1, true, new Timestamp(Calendar.getInstance().getTimeInMillis())));

        // cache hits
        b.add(new AttributeDescriptorImpl(SimpleSchema.LONG, new NameImpl(HINTS_NAME), 1, 1, true,
                0));

        final SimpleFeatureType newSchema = b.buildFeatureType();

        if (isDirty(name)) {
            // cache.createSchema(SimpleFeatureTypeBuilder.copy(store.getSchema(name)));
            cacheManager.getCache().updateSchema(name, newSchema);
        } else {
            cacheManager.getCache().createSchema(newSchema);
        }

        return newSchema;
    }

    @Override
    public SimpleFeatureType getCache(Name o) throws IOException {
        verify(o);
        return cacheManager.getCache().getSchema(o);
    }

    @Override
    public boolean isDirty(Name key) throws IOException {
        return false;
    }

    @Override
    public void setDirty(Name query) throws IOException {
        throw new UnsupportedOperationException();
    }

}
