package org.geotools.data.cache.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.cache.op.CacheOp;
import org.geotools.data.cache.op.CachedOp;
import org.geotools.data.cache.op.CachedOpSPI;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.ehcache.EhCacheCache;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component("CacheUtils")
public class CacheUtils implements ApplicationContextAware {

    @Autowired
    private List<CachedOpSPI<?>> catalog;

    private static ApplicationContext context;

    @Autowired
    private List<org.springframework.cache.CacheManager> caches;

    public Map<Object, CachedOp<?>> buildCache(final DataStore store, final DataStore cache)
            throws IOException {
        final Map<Object, CachedOp<?>> cacheMap = new HashMap<Object, CachedOp<?>>();
        for (CacheOp op : CacheOp.values()) {
            for (CachedOpSPI<?> spi : catalog) {
                if (spi.getOp().equals(op)) {
                    cacheMap.put(op, spi.create(store, cache));
                }
            }
        }
        return cacheMap;
    }

    public void printCache() {
        for (org.springframework.cache.CacheManager cache : caches) {
            for (String cacheName : cache.getCacheNames()) {
                org.springframework.cache.Cache c = cache.getCache(cacheName);
                System.out.print(((EhCacheCache) c.getNativeCache()));
            }
        }
    }

    public Object getCache(String name, Object key) {
        for (org.springframework.cache.CacheManager cache : caches) {
            for (String cacheName : cache.getCacheNames()) {
                if (name.equalsIgnoreCase(cacheName)) {
                    org.springframework.cache.Cache c = cache.getCache(cacheName);
                    return c.get(key);
                }
            }
        }
        return null;
    }
    
    public org.springframework.cache.Cache getCache(String name) {
        for (org.springframework.cache.CacheManager cache : caches) {
            for (String cacheName : cache.getCacheNames()) {
                if (name.equalsIgnoreCase(cacheName)) {
                    org.springframework.cache.Cache c = cache.getCache(cacheName);
                    return c;
                }
            }
        }
        return null;
    }

    public static CacheUtils getCacheUtils() {
        if (context!=null)
            return (CacheUtils) context.getBean("CacheUtils");
        else
            return null;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.context = applicationContext;
    }
}
