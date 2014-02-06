package org.geotools.data.cache.op.schema;

import java.io.IOException;

import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.BaseOp;
import org.geotools.data.cache.op.CachedOpStatus;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

public class SchemaOp extends BaseOp<Name, SimpleFeatureType> {


    public SchemaOp(CacheManager cacheManager, final CachedOpStatus<Name> status) throws IOException {
        super(cacheManager, status);
    }
//
//    @Override
//    public <E extends CachedOp<SimpleFeatureType, Name>> void clone(E obj) throws IOException {
//        super.clone(obj);
//
//        final SchemaOp op = (SchemaOp) obj;
//        isDirtyLock = op.isDirtyLock;
//        try {
//            isDirtyLock.writeLock().lock();
//            this.isDirty = op.isDirty;
//        } finally {
//            isDirtyLock.writeLock().unlock();
//        }
//
//    }

    @Override
    public SimpleFeatureType updateCache(Name name) throws IOException {

//        verify(name);

        SimpleFeatureType schema = cacheManager.getSource().getSchema(name);

        if (isDirty(name)) {
            try {
                cacheManager.getCache().updateSchema(name, schema);
            } catch (IOException ioe) {
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
//        verify(o);
        return cacheManager.getCache().getSchema(o);
    }


}
