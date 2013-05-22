package org.geotools.data.cache.util;

import java.util.HashMap;
import java.util.Map;

import org.geotools.data.cache.op.CacheOp;
import org.geotools.util.Utilities;

public class CacheManager<T> {

    private final T source;

    private final Map<Object, Boolean> policy;

    private final Map<Object, Object> cache;

    public CacheManager(T source) {
        this(source, null);
    }

    public CacheManager(T source, Map<Object, Boolean> policy) {
        // this(source, policy);
        this.source = source;

        // init policy
        if (policy == null) {
            this.policy = new HashMap<Object, Boolean>();
        } else {
            this.policy = policy;
        }

        // init cache
        this.cache = new HashMap<Object, Object>();
    }

    public void clearPolicy() {
        setAllPolicy(CacheOp.values(), false);
    }

    // Cache POLICY
    public boolean getPolicy(Object operation) {
        boolean policy = this.policy.get(operation);
        return source != null && policy;
    }

    public boolean setPolicy(Object operation, boolean policy) {
        return this.policy.put(operation, policy);
    }

    public void setAllPolicy(Object[] policy, boolean status) {
        for (Object o : policy) {
            this.policy.put(o, status);
        }
    }

    // Cache STATUS
    public boolean hasCachedOperation(Object operation) {
        // if we are here the operation is not cached
        return cache.containsKey(operation);
    }

    public boolean hasCachedOperation(CacheOp operation, Object key) {
        if (key!=null)
            return cache.containsKey(new Pair<CacheOp, Object>(operation,key));
        else
            return cache.containsKey(operation);
    }

    public void setCachedOperation(CacheOp operation, boolean value) {
        setCachedOperation(operation, null, value);
    }

    public void setCachedOperation(CacheOp operation, Object key, boolean value) {
        if (key != null)
            cache.put(new Pair<CacheOp, Object>(operation, key), value);
        else
            cache.put(operation, value);
    }

    public Object getCachedOperation(CacheOp operation) {
        return cache.get(operation);
    }
    
    public Object getCachedOperation(CacheOp operation, Object key) {
        if (key != null)
            return cache.get(new Pair<CacheOp, Object>(operation, key));
        else
            return cache.get(operation);
    }
    
    public void cacheRemove(CacheOp operation, Object key) {
        if (key != null)
            cache.remove(new Pair<CacheOp, Object>(operation, key));
        else
            cache.remove(operation);
    }

    public void cacheRemove(CacheOp operation) {
        cache.remove(operation);
    }

    public T getSource() {
        return source;
    }

    /**
     * Please be advised that this is a data object:
     * 
     * <ul>
     * <li>equals - is dependent on both source and target being equal.</li>
     * <li>hashcode - is dependent on the hashCode of source and target.</li>
     * </ul>
     * 
     * A Pair is considered ordered:
     * 
     * <blockquote>
     * 
     * <pre>
     * Pair pair1 = new Pair(&quot;a&quot;, &quot;b&quot;);
     * Pair pair2 = new Pair(&quot;b&quot;, &quot;a&quot;);
     * 
     * System.out.println(pair1.equals(pair2)); // prints false
     * </pre>
     * 
     * </blockquote>
     * 
     * {@link #createFromCoordinateReferenceSystemCodes}.
     */
    private static final class Pair<K1,K2> {
        private final K1 source;
        private final K2 target;

        public Pair(K1 source, K2 target) {
            this.source = source;
            this.target = target;
        }

        public int hashCode() {
            int code = 0;
            if (source != null)
                code = source.hashCode();
            if (target != null)
                code += target.hashCode() * 37;
            return code;
        }

        public boolean equals(final Object other) {
            if (other instanceof Pair) {
                final Pair that = (Pair) other;
                return Utilities.equals(this.source, that.source)
                        && Utilities.equals(this.target, that.target);
            }
            return false;
        }

        public String toString() {
            return source + " \u21E8 " + target;
        }
    }

}
