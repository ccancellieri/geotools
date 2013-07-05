package org.geotools.data.cache.utils;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component(EHCacheUtils.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
public class EHCacheUtils implements ApplicationContextAware {
    
    protected final static transient Logger LOGGER = org.geotools.util.logging.Logging
            .getLogger("org.geotools.data.cache.utils.EHCacheUtils");
    
    public final static String BEAN_NAME = "cached-datastore-ehCacheUtils";
    
    private transient static ApplicationContext context;

    @Autowired
    private transient List<org.springframework.cache.CacheManager> caches;

    public Object getFromCache(String name, Object key) {
        if (caches==null){
            LOGGER.log(Level.SEVERE, "Unable to get a valid list of context");
            return null;
        }
        for (org.springframework.cache.CacheManager cache : caches) {
            for (String cacheName : cache.getCacheNames()) {
                if (name.equalsIgnoreCase(cacheName)) {
                    org.springframework.cache.Cache c = cache.getCache(cacheName);
                    return c.get(key);
                }
            }
        }
        LOGGER.warning("No CacheManager found in context");
        return null;
    }

    public org.springframework.cache.Cache getCache(String name) {
        if (caches==null){
            LOGGER.log(Level.SEVERE, "Unable to get a valid list of context");
            return null;
        }
        for (org.springframework.cache.CacheManager cache : caches) {
            for (String cacheName : cache.getCacheNames()) {
                if (name.equalsIgnoreCase(cacheName)) {
                    org.springframework.cache.Cache c = cache.getCache(cacheName);
                    return c;
                }
            }
        }
        LOGGER.warning("No CacheManager found in context");
        return null;
    }

    public static EHCacheUtils getCacheUtils() {
        if (context != null) {
            return (EHCacheUtils) context.getBean(EHCacheUtils.class);
        } else {
            LOGGER.log(Level.SEVERE, "Unable to get a valid context");
        }
        return null;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext;
    }

}
