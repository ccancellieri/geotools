package org.geotools.data.cache.op;

public abstract class BaseOpSPI<S extends CachedOpStatus<K>, E extends CachedOp<K, T>, K, T> extends
        CachedOpSPI<S, E, K, T> {

    /** serialVersionUID */
    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unchecked")
    public S createStatus() {
        return (S) new BaseOpStatus<K>() {
            @Override
            public boolean isApplicable(Operation op) {
                if (op.equals(getOp())) {
                    return true;
                }
                return false;
            }
        };
    }

    /**
     * @return The cached operation priority
     */
    public long priority() {
        return 0L;
    }

}
