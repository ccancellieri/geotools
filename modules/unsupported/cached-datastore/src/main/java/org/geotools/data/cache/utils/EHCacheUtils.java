package org.geotools.data.cache.utils;

import java.util.logging.Level;
import java.util.logging.Logger;

import net.sf.ehcache.CacheManager;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.cache.ehcache.EhCacheCache;
import org.springframework.cache.ehcache.EhCacheCacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component(EHCacheUtils.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
public class EHCacheUtils implements ApplicationContextAware, DisposableBean {

    private final static transient Logger LOGGER = org.geotools.util.logging.Logging
            .getLogger("org.geotools.data.cache.utils.EHCacheUtils");

    public final static String BEAN_NAME = "cached-datastore-ehCacheUtils";
    
    static {
        System.setProperty(CacheManager.ENABLE_SHUTDOWN_HOOK_PROPERTY,"true");
    }

    private transient static ApplicationContext context;

    @Autowired
    private transient static org.springframework.cache.CacheManager manager;
    

    public static final String CACHEMANAGER_STORE_NAME = "CacheManagerStatus";

    // storage for the cache status
    private final transient static EhCacheCache ehcache = EHCacheUtils.getCacheUtils().getCacheOfType(
            CACHEMANAGER_STORE_NAME, EhCacheCache.class);

    public static <K> void evict(K key) {
        // evict
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Evicting " + key + " from storage");
        }
        ehcache.evict(key.hashCode());
    }

    public static <K, T> void store(K key, T value) {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Storing " + key + " into cache using (" + key.hashCode() + ")");
        }
        ehcache.put(key.hashCode(), value);
    }

    public static <K, T> T load(K key) {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Loading " + key + " from cache (" + key.hashCode() + ")");
        }
        final ValueWrapper cachedStatus = ehcache.get(key.hashCode());
        if (cachedStatus != null) {
            return (T) cachedStatus.get();
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.log(Level.WARNING, "No entry load from cache for key: " + key);
            }
        }
        return null;
    }
    

    public static org.springframework.cache.Cache getCache(String name) {
        if (manager == null) {
            LOGGER.log(Level.SEVERE, "Unable to get a valid list of context");
            return null;
        }
        return manager.getCache(name);
    }

    public static <T extends Cache> T getCacheOfType(String name, Class<T> clazz) {
        Cache c = getCache(name);
        if (c != null) {
            return (T) c;
        } else {
            return null;
        }
    }

    public static org.springframework.cache.CacheManager getCacheManager() {
        if (manager == null) {
            LOGGER.log(Level.SEVERE, "Unable to get a valid list of context");
            return null;
        }
        return manager;
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

    @Override
    public void destroy() throws Exception {
        if (manager instanceof EhCacheCacheManager) {
            LOGGER.info("shutting down the EhCacheManager");
            ((EhCacheCacheManager) manager).getCacheManager().shutdown();
        } else {
            LOGGER.log(Level.SEVERE,"Unable to shutdown the CacheManager");
        }
    }

}
