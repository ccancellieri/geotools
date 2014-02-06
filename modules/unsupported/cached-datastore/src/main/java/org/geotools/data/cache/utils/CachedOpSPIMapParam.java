package org.geotools.data.cache.utils;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.DataAccessFactory.Param;
import org.geotools.data.cache.op.CachedOp;
import org.geotools.data.cache.op.CachedOpSPI;
import org.opengis.util.InternationalString;

/**
 * 
 * @author carlo cancellieri - GeoServer SAS
 * 
 */
public class CachedOpSPIMapParam extends Param {

    protected final static transient Logger LOGGER = org.geotools.util.logging.Logging
            .getLogger("org.geotools.data.cache.utils.CachedOpSPIMapParam");

    public CachedOpSPIMapParam(String arg0) {
        super(arg0);
    }

    public CachedOpSPIMapParam(String arg0, Class<?> arg1) {
        super(arg0, arg1);
    }

    public CachedOpSPIMapParam(String arg0, Class<?> arg1, String arg2) {
        super(arg0, arg1, arg2);
    }

    public CachedOpSPIMapParam(String arg0, Class<?> arg1, String arg2, boolean arg3) {
        super(arg0, arg1, arg2, arg3);
    }

    public CachedOpSPIMapParam(String arg0, Class<?> arg1, String arg2, boolean arg3, Object arg4) {
        super(arg0, arg1, arg2, arg3, arg4);
    }

    public CachedOpSPIMapParam(String arg0, Class<?> arg1, InternationalString arg2, boolean arg3,
            Object arg4) {
        super(arg0, arg1, arg2, arg3, arg4);
    }

    public CachedOpSPIMapParam(String arg0, Class<?> arg1, String arg2, boolean arg3, Object arg4,
            Map<String, ?> arg5) {
        super(arg0, arg1, arg2, arg3, arg4, arg5);
    }

    public CachedOpSPIMapParam(String arg0, Class<?> arg1, String arg2, boolean arg3, Object arg4,
            Object... arg5) {
        super(arg0, arg1, arg2, arg3, arg4, arg5);
    }

    public CachedOpSPIMapParam(String arg0, Class<?> arg1, InternationalString arg2, boolean arg3,
            Object arg4, Map<String, ?> arg5) {
        super(arg0, arg1, arg2, arg3, arg4, arg5);
    }

    @Override
    public Object parse(String text) {
        return parseSPIMap(text);
    }

    public static <T, K> Map<String, CachedOpSPI<CachedOp<K, T>, K, T>> parseSPIMap(String text) {
        final Map<String, CachedOpSPI<CachedOp<K, T>, K, T>> output = new HashMap<String, CachedOpSPI<CachedOp<K, T>, K, T>>();
        text = text.substring(1, text.length() - 1);
        for (String pair : text.split(",")) {
            final String[] kv = pair.split("=");
            if (kv.length > 1 && kv[1].length() > 0) {
                CachedOpSPI<CachedOp<K, T>, K, T> spi;
                try {
                    spi = (CachedOpSPI<CachedOp<K, T>, K, T>) Class.forName(kv[1].trim())
                            .newInstance();
                    output.put(kv[0].trim(), spi);
                } catch (InstantiationException e) {
                    LOGGER.log(Level.SEVERE, e.getMessage(), e);
                } catch (IllegalAccessException e) {
                    LOGGER.log(Level.SEVERE, e.getMessage(), e);
                } catch (ClassNotFoundException e) {
                    LOGGER.log(Level.SEVERE, e.getMessage(), e);
                }
            }
        }
        return output;
    }

    @Override
    public String text(Object value) {
        return toText((Map<String, CachedOpSPI<?, ?, ?>>) value);
    }

    public static <T, K> String toText(Map<String, CachedOpSPI<?, ?, ?>> value) {
        if (value == null)
            throw new IllegalArgumentException("Unable to convert a null map");
        final StringWriter sw = new StringWriter();
        sw.write('{');
        final Iterator<Entry<String, CachedOpSPI<?, ?, ?>>> it = value.entrySet().iterator();
        if (it.hasNext()) {
            Entry<String, CachedOpSPI<?, ?, ?>> e = it.next();
            if (e.getValue() != null) {
                sw.write(e.getKey());
                sw.write('=');
                sw.write(e.getValue().getClass().getName());
            }
        }
        while (it.hasNext()) {
            Entry<String, CachedOpSPI<?, ?, ?>> e = it.next();
            if (e.getValue() != null) {
                sw.write(',');
                sw.write(e.getKey());
                sw.write('=');
                sw.write(e.getValue().getClass().getName());
            }
        }
        sw.write('}');
        return sw.toString();
    }

}
