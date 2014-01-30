package org.geotools.data.cache.utils;

import java.util.logging.Level;
import java.util.logging.Logger;

import net.sf.ehcache.CacheManager;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.cache.Cache;
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
    private transient org.springframework.cache.CacheManager manager;

    public org.springframework.cache.Cache getCache(String name) {
        if (manager == null) {
            LOGGER.log(Level.SEVERE, "Unable to get a valid list of context");
            return null;
        }
        return manager.getCache(name);
    }

    public <T extends Cache> T getCacheOfType(String name, Class<T> clazz) {
        Cache c = getCache(name);
        if (c != null) {
            return (T) c;
        } else {
            return null;
        }
    }

    public org.springframework.cache.CacheManager getCacheManager() {
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
