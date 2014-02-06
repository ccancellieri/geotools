package org.geotools.data.cache.utils;

import java.io.StringWriter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.cache.op.BaseOp;
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
    private transient List<CachedOpSPI<BaseOp<?, ?>, ?, ?>> catalog;

    public List<CachedOpSPI<BaseOp<?, ?>, ?, ?>> getCachedOps() {
        return catalog;
    }

    public TreeSet<CachedOpSPI<BaseOp<?, ?>, ?, ?>> getCachedOpSPITree(Operation op) {
        final TreeSet<CachedOpSPI<BaseOp<?, ?>, ?, ?>> tree = new TreeSet<CachedOpSPI<BaseOp<?, ?>, ?, ?>>(
                new Comparator<CachedOpSPI<BaseOp<?, ?>, ?, ?>>() {
                    @Override
                    public int compare(CachedOpSPI<BaseOp<?, ?>, ?, ?> o1,
                            CachedOpSPI<BaseOp<?, ?>, ?, ?> o2) {
                        return o1.priority() > o2.priority() ? 1 : -1;
                    }
                });
        for (CachedOpSPI<BaseOp<?, ?>, ?, ?> spi : getCachedOps()) {
            if (spi.getOp().equals(op)) {
                tree.add(spi);
            }
        }
        return tree;
    }

    public CachedOpSPI<BaseOp<?, ?>, ?, ?> getFirstCachedOpSPI(Operation op) {
        final TreeSet<CachedOpSPI<BaseOp<?, ?>, ?, ?>> spiTree = getCachedOpSPITree(op);
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
            if (kv.length == 2 && kv[0] != null && !kv[0].isEmpty() && kv[1] != null
                    && !kv[1].isEmpty()) {
                map.put(kv[0].trim(), kv[1].trim());
            }
        }
        return map;
    }

    public static <T> String toText(Map<String, T> value) {
        if (value == null)
            throw new IllegalArgumentException("Unable to convert a null map");
        final StringWriter sw = new StringWriter();
        sw.write('{');
        final Iterator<Entry<String, T>> it = value.entrySet().iterator();
        if (it.hasNext()) {
            Entry<String, T> e = it.next();
            if (e.getValue() != null) {
                sw.write(e.getKey().trim());
                sw.write('=');
                sw.write(e.getValue().toString().trim());
            }
        }
        while (it.hasNext()) {
            Entry<String, T> e = it.next();
            if (e.getValue() != null) {
                sw.write(',');
                sw.write(e.getKey().trim());
                sw.write('=');
                sw.write(e.getValue().toString().trim());
            }
        }
        sw.write('}');
        return sw.toString();
    }

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

}
