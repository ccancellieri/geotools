package org.geotools.data.cache.utils;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.geotools.data.DataAccessFactory.Param;
import org.opengis.util.InternationalString;

public class MapParam extends Param {

    public MapParam(String arg0, Class<?> arg1, InternationalString arg2, boolean arg3,
            Object arg4, Map<String, ?> arg5) {
        super(arg0, arg1, arg2, arg3, arg4, arg5);
    }

    public MapParam(String arg0, Class<?> arg1, InternationalString arg2, boolean arg3, Object arg4) {
        super(arg0, arg1, arg2, arg3, arg4);
    }

    public MapParam(String arg0, Class<?> arg1, String arg2, boolean arg3, Object arg4,
            Map<String, ?> arg5) {
        super(arg0, arg1, arg2, arg3, arg4, arg5);
    }

    public MapParam(String arg0, Class<?> arg1, String arg2, boolean arg3, Object arg4,
            Object... arg5) {
        super(arg0, arg1, arg2, arg3, arg4, arg5);
    }

    public MapParam(String arg0, Class<?> arg1, String arg2, boolean arg3, Object arg4) {
        super(arg0, arg1, arg2, arg3, arg4);
    }

    public MapParam(String arg0, Class<?> arg1, String arg2, boolean arg3) {
        super(arg0, arg1, arg2, arg3);
    }

    public MapParam(String arg0, Class<?> arg1, String arg2) {
        super(arg0, arg1, arg2);
    }

    public MapParam(String arg0, Class<?> arg1) {
        super(arg0, arg1);
    }

    public MapParam(String arg0) {
        super(arg0);
    }

    /** serialVersionUID */
    private static final long serialVersionUID = -4043222059380622418L;

    @Override
    public Object parse(String text) throws IOException {
        return parseMap(text);
    }

    @Override
    public String text(Object value) {
        return value.toString();
    }

    public static Map<String, Serializable> parseMap(String input) {
        final Map<String, Serializable> map = new HashMap<String, Serializable>();
        if (input == null)
            return map;
        input = input.substring(1, input.length() - 1);
        for (String pair : input.split(",")) {
            if (pair != null && pair.length() > 1) {
                final String[] kv = pair.split("=");
                if (kv != null && kv.length == 2) {
                    map.put(kv[0].trim(), kv[1].trim());
                }
            }
        }
        return map;
    }
}