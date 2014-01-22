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

public class SchemaOp extends BaseSchemaOp<Name> {

    public SchemaOp(CacheManager cacheManager, final String uniqueName) throws IOException {
        super(cacheManager, uniqueName);
    }

    @Override
    public SimpleFeatureType updateCache(Name name) throws IOException {

        verify(name);

        SimpleFeatureType schema = cacheManager.getSource().getSchema(name);

        if (isDirty(name)) {
            cacheManager.getCache().updateSchema(name, schema);
        } else {
            cacheManager.getCache().createSchema(schema);
        }

        return schema;
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
