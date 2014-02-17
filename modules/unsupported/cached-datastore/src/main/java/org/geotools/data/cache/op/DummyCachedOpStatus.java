package org.geotools.data.cache.op;

import java.io.IOException;
import java.io.Serializable;

public class DummyCachedOpStatus<K> implements CachedOpStatus<K>, Serializable {

    /** serialVersionUID */
    private static final long serialVersionUID = 1L;

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
