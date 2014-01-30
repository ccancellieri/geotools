package org.geotools.data.cache.op;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

public class SchemaOp extends BaseSchemaOp<Name> {

    private Map<Name, Boolean> isDirty = new HashMap<Name, Boolean>();

    private ReentrantReadWriteLock isDirtyLock = new ReentrantReadWriteLock();

    public SchemaOp(CacheManager cacheManager, final String uniqueName) throws IOException {
        super(cacheManager, uniqueName);
    }

    @Override
    public <E extends CachedOp<SimpleFeatureType, Name>> void clone(E obj) throws IOException {
        super.clone(obj);

        final SchemaOp op = (SchemaOp) obj;
        isDirtyLock = op.isDirtyLock;
        try {
            isDirtyLock.writeLock().lock();
            this.isDirty=op.isDirty;
        } finally {
            isDirtyLock.writeLock().unlock();
        }

    }

    @Override
    public SimpleFeatureType updateCache(Name name) throws IOException {

        verify(name);

        SimpleFeatureType schema = cacheManager.getSource().getSchema(name);

        if (isDirty(name)) {
            try {
                cacheManager.getCache().updateSchema(name, schema);
            } catch (IOException ioe){
                //cacheManager.getCache().dropSchema(name, schema);
//                cacheManager.getCache().createSchema(schema);
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
        verify(o);
        return cacheManager.getCache().getSchema(o);
    }

    @Override
    public boolean isDirty(Name key) throws IOException {
        try {
            isDirtyLock.readLock().lock();
            return isDirty.containsKey(key);
        } finally {
            isDirtyLock.readLock().unlock();
        }

    }

    @Override
    public void setDirty(Name name, boolean value) throws IOException {
        try {
            isDirtyLock.writeLock().lock();
            isDirty.put(name, value);
        } finally {
            isDirtyLock.writeLock().unlock();
        }
    }

    @Override
    public void clear() throws IOException {
        try {
            isDirtyLock.writeLock().lock();
            isDirty.clear();
        } finally {
            isDirtyLock.writeLock().unlock();
        }
        super.clear();
    }

}
