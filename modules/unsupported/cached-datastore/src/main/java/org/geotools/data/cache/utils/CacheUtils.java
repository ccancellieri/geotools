package org.geotools.data.cache.utils;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.cache.op.CachedOp;
import org.geotools.data.cache.op.CachedOpSPI;
import org.geotools.data.cache.op.Operation;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component(CacheUtils.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
public class CacheUtils implements ApplicationContextAware {

    protected final static transient Logger LOGGER = org.geotools.util.logging.Logging
            .getLogger("org.geotools.data.cache.utils.CacheUtils");

    public final static String BEAN_NAME = "cached-datastore-cacheUtils";

    private static transient ApplicationContext context;

    @Autowired
    private transient List<CachedOpSPI<CachedOp<?, ?, ?>>> catalog;

    public List<CachedOpSPI<CachedOp<?, ?, ?>>> getCachedOps() {
        return catalog;
    }

    public TreeSet<CachedOpSPI<CachedOp<?, ?, ?>>> getCachedOpSPITree(Operation op) {
        final TreeSet<CachedOpSPI<CachedOp<?, ?, ?>>> tree = new TreeSet<CachedOpSPI<CachedOp<?, ?, ?>>>(
                new Comparator<CachedOpSPI<CachedOp<?, ?, ?>>>() {
                    @Override
                    public int compare(CachedOpSPI<CachedOp<?, ?, ?>> o1,
                            CachedOpSPI<CachedOp<?, ?, ?>> o2) {
                        return o1.priority() > o2.priority() ? 1 : -1;
                    }
                });
        for (CachedOpSPI<CachedOp<?, ?, ?>> spi : getCachedOps()) {
            if (spi.getOp().equals(op)) {
                tree.add(spi);
            }
        }
        return tree;
    }

    public CachedOpSPI<CachedOp<?, ?, ?>> getFirstCachedOpSPI(Operation op) {
        final TreeSet<CachedOpSPI<CachedOp<?, ?, ?>>> spiTree = getCachedOpSPITree(op);
        if (!spiTree.isEmpty()) {
            return spiTree.first();
        } else {
            LOGGER.log(Level.WARNING, "Unable to locate a CachedOpSPI for Operation: " + op);
        }
        return null;
    }

    public static Map<String, String> parseMap(String input) {
        final Map<String, String> map = new HashMap<String, String>();
        input = input.substring(1, input.length() - 1);
        for (String pair : input.split(",")) {
            String[] kv = pair.split("=");
            map.put(kv[0].trim(), kv[1].trim());
        }
        return map;
    }

    //
    // public void printCache() {
    // for (org.springframework.cache.CacheManager cache : caches) {
    // for (String cacheName : cache.getCacheNames()) {
    // org.springframework.cache.Cache c = cache.getCache(cacheName);
    // System.out.print(((EhCacheCache) c.getNativeCache()));
    // }
    // }
    // }
    //
    // public Object getCache(String name, Object key) {
    // for (org.springframework.cache.CacheManager cache : caches) {
    // for (String cacheName : cache.getCacheNames()) {
    // if (name.equalsIgnoreCase(cacheName)) {
    // org.springframework.cache.Cache c = cache.getCache(cacheName);
    // return c.get(key);
    // }
    // }
    // }
    // return null;
    // }
    //
    // public org.springframework.cache.Cache getCache(String name) {
    // for (org.springframework.cache.CacheManager cache : caches) {
    // for (String cacheName : cache.getCacheNames()) {
    // if (name.equalsIgnoreCase(cacheName)) {
    // org.springframework.cache.Cache c = cache.getCache(cacheName);
    // return c;
    // }
    // }
    // }
    // return null;
    // }

    public static CacheUtils getCacheUtils() {
        if (context != null)
            return (CacheUtils) context.getBean(CacheUtils.class);
        else {
            LOGGER.log(Level.SEVERE, "Unable to get a valid context");
        }
        return null;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.context = applicationContext;
    }
    //
    // @Override
    // public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
    // // for (String beanName : dependencies.keySet()) {
    // String beanName="geoServerLoader";
    // BeanDefinition bd = beanFactory.getBeanDefinition(beanName);
    //
    // bd.setDependsOn(StringUtils.mergeStringArrays(bd.getDependsOn(), new String[]{BEAN_NAME,EHCacheUtils.BEAN_NAME}));
    // // }
    //
    // }

}
