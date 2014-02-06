package org.geotools.data.cache.op;

import java.io.IOException;
import java.util.List;

public class DummyCachedOpStatus<K> implements CachedOpStatus<K> {

    @Override
    public void clear() throws IOException {
    }

    @Override
    public boolean isDirty(K key) throws IOException {
        return false;
    }

    @Override
    public void setDirty(K key, boolean value) throws IOException {
    }

    @Override
    public boolean isCached(K key) throws IOException {
        return true;
    }

    @Override
    public void setCached(K key, boolean isCached) throws IOException {
    }

    @Override
    public boolean isApplicable(Operation op) {
        return true;
    }

}
