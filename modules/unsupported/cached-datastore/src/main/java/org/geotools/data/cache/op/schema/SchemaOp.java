package org.geotools.data.cache.op.schema;

import java.io.IOException;

import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.BaseOp;
import org.geotools.data.cache.op.CachedOpStatus;
import org.geotools.data.cache.utils.EHCacheUtils;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

public class SchemaOp extends BaseOp<CachedOpStatus<Name>, Name, SimpleFeatureType> {

    public SchemaOp(CacheManager cacheManager, final CachedOpStatus<Name> status)
            throws IOException {
        super(cacheManager, status);
    }

    @Override
    public SimpleFeatureType updateCache(Name name) throws IOException {

        SimpleFeatureType schema = cacheManager.getSource().getSchema(name);

        if (isDirty(name)) {
            try {
                cacheManager.getCache().updateSchema(name, schema);
            } catch (IOException ioe) {
                // TODO only on > GT-11.x
                // cacheManager.getCache().dropSchema(name, schema);
                // cacheManager.getCache().createSchema(schema);
                throw new UnsupportedOperationException("we need drop schema");
            }
            setDirty(name, false);
        } else {
            cacheManager.getCache().createSchema(schema);
        }
        setCached(name, true);

        return schema;
    }

    @Override
    public SimpleFeatureType getCache(Name o) throws IOException {
        // verify(o);
        return cacheManager.getCache().getSchema(o);
    }

    @Override
    public void save() throws IOException {
        cacheManager.save();
        EHCacheUtils.flush();
    }
}
