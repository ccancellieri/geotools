/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2002-2009, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package org.geotools.data.cache.datastore;

import java.awt.RenderingHints.Key;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import net.sf.ehcache.transaction.xa.commands.StorePutCommand;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.Transformer;
import org.geotools.data.AbstractDataStoreFactory;
import org.geotools.data.DataAccess;
import org.geotools.data.DataAccessFactory;
import org.geotools.data.DataAccessFinder;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.data.Repository;
import org.geotools.data.cache.impl.STRFeatureSourceOpSPI;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.cache.op.CachedOp;
import org.geotools.feature.NameImpl;
import org.geotools.util.KVP;
import org.opengis.feature.Feature;
import org.opengis.feature.type.FeatureType;
import org.opengis.util.InternationalString;

/**
 * 
 * 
 * @source $URL$
 */
public class CachedDataStoreFactory extends AbstractDataStoreFactory implements DataStoreFactorySpi {

    public final static String STORE_NAME = "CachedDataStore";

    public final static String STORE_DESC = "Add a cache layer to the selected datastore (the cache can be a local datastore or a spring pluggable cache)";

    public static final Param REPOSITORY_PARAM = new Param("repository", Repository.class,
            "The repository that will provide the store intances", true, null, new KVP(Param.LEVEL,
                    "advanced"));

    public static final Param NAMESPACE = new Param("Namespace", String.class, "Namespace prefix",
            true, "topp", new KVP(Param.ELEMENT, String.class));

    public static final String SOURCE_TYPE_KEY = "SourceNameType";

    public static final Param SOURCE_TYPE = new Param(SOURCE_TYPE_KEY, String.class,
            "Setup a storage type for the source (use cache for spring cache)", true, null);

    public static final String SOURCE_PARAMS_KEY = "SourceParams";

    public static final Param SOURCE_PARAMS = new MapParam(SOURCE_PARAMS_KEY, Map.class,
            "The target data store to cache", true, null, new KVP(Param.ELEMENT, String.class));

    public static final String CACHE_TYPE_KEY = "CacheNameType";

    public static final Param CACHE_TYPE = new Param(CACHE_TYPE_KEY, String.class,
            "Setup the cache type (use cache for spring cache)", true, null);

    public static final String CACHE_PARAMS_KEY = "CacheParams";

    public static final Param CACHE_PARAMS = new MapParam(CACHE_PARAMS_KEY, Map.class,
            "The target data store to cache", true, null, new KVP(Param.ELEMENT, String.class));

    // public static final Param STORE = new Param("Store", String.class,
    // "The target data store to cache", true, null, new KVP(Param.ELEMENT, String.class));

    // public static final Param CACHE_PARAMS = new MapParam("Cache params array", Map.class,
    // "Pass parameters to the cache datastore", false, new KVP(Param.ELEMENT, Map.class));

    // public static final Param PARALLELISM = new Param("parallelism", Integer.class,
    // "Number of allowed concurrent queries on the delegate stores (unlimited by default)",
    // false, new Integer(-1));

    public String getDisplayName() {
        return STORE_NAME;
    }

    public String getDescription() {
        return STORE_DESC;
    }

    public Param[] getParametersInfo() {
        return new Param[] { REPOSITORY_PARAM, SOURCE_TYPE, SOURCE_PARAMS, CACHE_TYPE,
                CACHE_PARAMS, NAMESPACE };
    }

    public boolean isAvailable() {
        return true;
    }

    public Map<Key, ?> getImplementationHints() {
        return null;
    }

    public CachedDataStoreFactory() {
        super();
    }

    private static class MapParam extends Param {

        public MapParam(String arg0, Class<?> arg1, InternationalString arg2, boolean arg3,
                Object arg4, Map<String, ?> arg5) {
            super(arg0, arg1, arg2, arg3, arg4, arg5);
        }

        public MapParam(String arg0, Class<?> arg1, InternationalString arg2, boolean arg3,
                Object arg4) {
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

        private Map<String, Serializable> parseMap(String input) {
            final Map<String, Serializable> map = new HashMap<String, Serializable>();
            input = input.substring(1, input.length() - 1);
            for (String pair : input.split(",")) {
                String[] kv = pair.split("=");
                map.put(kv[0], kv[1]);
            }
            return map;
        }
    }

    /**
     * @param params
     * @param prefix
     * @return a map (subset of the passed params map) containing all the objects having the key matching the prefix.*. The resulting keys are purged
     *         from the prefix.
     */
    public static Map<String, Serializable> extractParams(final Map<String, Serializable> params,
            final String prefix) {
        final Map<String, Serializable> ret = new HashMap<String, Serializable>();
        for (String key : params.keySet()) {
            String subKey = key.replaceFirst(prefix + ".*", "");
            ret.put(subKey, params.get(key));
        }
        return ret;
    }

    /**
     * appends a prefix to all the keys in the params map, returning the transformed map
     * 
     * @param params
     * @param prefix
     * @return
     */
    public static Map<String, Serializable> addParams(final Map<String, Serializable> params,
            final String prefix) {
        final Map<String, Serializable> ret = MapUtils.transformedMap(params, new Transformer() {
            @Override
            public Object transform(Object input) {
                return new StringBuilder().append(prefix).append(input).toString();
            }
        }, null);
        ret.putAll(params);
        return ret;
    }

    public DataStore createDataStore(Map<String, Serializable> params) throws IOException {
        // Repository repository = lookup(REPOSITORY_PARAM, params, Repository.class);

        // String namespace = lookup(NAMESPACE, params, String.class);

        final String sourceType = lookup(SOURCE_TYPE, params, String.class);
        final Map<String, Serializable> sourceParams = lookup(SOURCE_PARAMS, params, Map.class);
        // DataStore source = repository.dataStore(new NameImpl(namespace, store));
        final DataStore source = (DataStore) getDataStore(sourceParams, sourceType);

        //
        // ExecutorService executor;
        // int parallelism = lookup(PARALLELISM, params, Integer.class);
        // if (parallelism <= 0) {
        // executor = Executors.newCachedThreadPool();
        // } else {
        // executor = Executors.newFixedThreadPool(parallelism);
        // }

        // DataStore source = repository.dataStore(new NameImpl(namespace, store));

        // CacheManagerOld<DataStore> manager = null;
        final String cacheType = lookup(CACHE_TYPE, params, String.class);
        final Map<String, Serializable> cacheParams = lookup(CACHE_PARAMS, params, Map.class);
        
        final DataStore cache = (DataStore) getDataStore(sourceParams, sourceType);
        final CacheManager props = new CacheManager(source,cache);

        if (cache == null) {
            props.putCachedOp(Operation.featureSource, new STRFeatureSourceOpSPI().create(source, null,props));
            // cache = new SpringCachedDataStore(source);
            // manager = new CacheManagerOld<DataStore>(cache);
            // manager.setAllPolicy(CacheOp.values(), true);
            return new CachedDataStore(source, null, props);
        }
        
        return new CachedDataStore(source, cache, null);
    }

    /**
     * @return the name/description set of available datastore factories
     */
    public static Map<String, DataAccessFactory> getAvailableDataStores() {
        Iterator<DataAccessFactory> availableDataStores = DataAccessFinder.getAvailableDataStores();
        final Map<String, DataAccessFactory> storeNames = new HashMap<String, DataAccessFactory>();

        while (availableDataStores.hasNext()) {
            DataAccessFactory factory = availableDataStores.next();
            String name = factory.getDisplayName();
            if (name != null && !name.isEmpty()// ) {
                    && !name.equalsIgnoreCase(CachedDataStoreFactory.STORE_NAME)) {
                storeNames.put(factory.getDisplayName(), factory);
            }
        }
        return storeNames;
    }

    private static DataAccess<? extends FeatureType, ? extends Feature> getDataStore(
            final Map<String, Serializable> existingParameters, final String factoryName)
            throws IOException {

        final DataAccessFactory storeFactory = getAvailableDataStores().get(factoryName);

        if (storeFactory != null && storeFactory.canProcess(existingParameters)) {
            return storeFactory.createDataStore(existingParameters);
        }
        throw new IOException("Unable to create a new store using passed existingParameters");
    }

    @Override
    public DataStore createNewDataStore(Map<String, Serializable> params) throws IOException {
        return createDataStore(params);
    }

    /**
     * Looks up a parameter, if not found it returns the default value, assuming there is one, or null otherwise
     * 
     * @param <T>
     * @param param
     * @param params
     * @param target
     * @return
     * @throws IOException
     */
    <T> T lookup(Param param, Map<String, Serializable> params, Class<T> target) throws IOException {
        T result = (T) param.lookUp(params);
        if (result == null) {
            return (T) param.getDefaultValue();
        } else {
            return result;
        }

    }

}
