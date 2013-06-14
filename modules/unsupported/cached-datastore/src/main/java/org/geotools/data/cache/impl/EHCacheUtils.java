package org.geotools.data.cache.impl;

import java.util.List;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.ehcache.EhCacheCache;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component("EhCacheUtils")
public class EHCacheUtils implements ApplicationContextAware {

    private static ApplicationContext context;

    @Autowired
    private List<org.springframework.cache.CacheManager> caches;

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

    public static EHCacheUtils getCacheUtils() {
        if (context != null)
            return (EHCacheUtils) context.getBean("EhCacheUtils");
        else
            return null;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.context = applicationContext;
    }
}
