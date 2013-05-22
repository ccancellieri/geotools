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

import org.geotools.data.AbstractDataStoreFactory;
import org.geotools.data.DataAccessFactory;
import org.geotools.data.DataAccessFinder;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.data.Repository;
import org.geotools.data.cache.impl.STRFeatureSourceOpSPI;
import org.geotools.data.cache.op.CacheOp;
import org.geotools.data.cache.op.CachedOp;
import org.geotools.data.cache.op.CachedOpSPI;
import org.geotools.feature.NameImpl;
import org.geotools.util.KVP;

/**
 * 
 * 
 * @source $URL$
 */
public class CachedDataStoreFactory extends AbstractDataStoreFactory implements DataStoreFactorySpi {

    public static final Param REPOSITORY_PARAM = new Param("repository", Repository.class,
            "The repository that will provide the store intances", true, null, new KVP(Param.LEVEL,
                    "advanced"));

    public static final Param NAMESPACE = new Param("Namespace", String.class, "Namespace prefix",
            true, "topp", new KVP(Param.ELEMENT, String.class));

    public static final Param STORE = new Param("Store", String.class,
            "The target data store to cache", true, null, new KVP(Param.ELEMENT, String.class));

    public static final Param CACHE_TYPE = new Param("Cache type name", String.class,
            "Setup a storage system for the cache (use cache for spring cache)", true, "cache");

    public static final Param CACHE_PARAMS = new Param("Cache params array", Map.class,
            "Pass parameters to the cache datastore", false, new KVP(Param.ELEMENT, Map.class)) {

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
    };

    // public static final Param PARALLELISM = new Param("parallelism", Integer.class,
    // "Number of allowed concurrent queries on the delegate stores (unlimited by default)",
    // false, new Integer(-1));

    public String getDisplayName() {
        return "Cached data store";
    }

    public String getDescription() {
        return "Add a cache layer to the selected datastore (the cache can be a local datastore or a spring pluggable cache)";
    }

    public Param[] getParametersInfo() {
        return new Param[] { REPOSITORY_PARAM, STORE, NAMESPACE, CACHE_TYPE, CACHE_PARAMS };
    }

    public boolean isAvailable() {
        return true;
    }

    public Map<Key, ?> getImplementationHints() {
        return null;
    }

    public DataStore createDataStore(Map<String, Serializable> params) throws IOException {
        Repository repository = lookup(REPOSITORY_PARAM, params, Repository.class);
        String store = lookup(STORE, params, String.class);
        String cacheType = lookup(CACHE_TYPE, params, String.class);
        Map<String, Serializable> cacheParams = lookup(CACHE_PARAMS, params, Map.class);
        String namespace = lookup(NAMESPACE, params, String.class);
        //
        // ExecutorService executor;
        // int parallelism = lookup(PARALLELISM, params, Integer.class);
        // if (parallelism <= 0) {
        // executor = Executors.newCachedThreadPool();
        // } else {
        // executor = Executors.newFixedThreadPool(parallelism);
        // }

        DataStore source = repository.dataStore(new NameImpl(namespace, store));
        
//        CacheManagerOld<DataStore> manager = null;
        Map<Object, CachedOp<?>> props = new HashMap<Object, CachedOp<?>>();
        DataStore cache = null;
//        if (!cacheType.equals("cache")) {
//            Collection<DataAccessFactory> factories = getAvailableDataStoreFactories();
//            for (DataAccessFactory factory : factories) {
//                if (factory.getDisplayName().equalsIgnoreCase(cacheType)) {
//                    Map<String, Serializable> props = new HashMap<String, Serializable>();
//                    for (Param p : factory.getParametersInfo()) {
//                        props.put(p.getName(), (Serializable) p.getDefaultValue());
//                    }
//                    // override
//                    props.putAll(cacheParams);
//                    cache = (ContentDataStore) factory.createDataStore(props);
//                    manager = new CacheManagerOld<DataStore>(cache);
//                }
//            }
//        }
        if (cache == null) {
            props.put(CacheOp.featureSource, new STRFeatureSourceOpSPI().create(source, null));
//            cache = new SpringCachedDataStore(source);
//            manager = new CacheManagerOld<DataStore>(cache);
//            manager.setAllPolicy(CacheOp.values(), true);
            return new CachedDataStore(source, null, props);
        }
        return new CachedDataStore(source, null);
    }

    public static Collection<DataAccessFactory> getAvailableDataStoreFactories() {
        List<DataAccessFactory> factories = new ArrayList();
        Iterator<DataAccessFactory> it = DataAccessFinder.getAvailableDataStores();
        while (it.hasNext()) {
            factories.add(it.next());
        }

        // for (DataAccessFactoryProducer producer : GeoServerExtensions.extensions(DataAccessFactoryProducer.class)) {
        // try {
        // factories.addAll(producer.getDataStoreFactories());
        // }
        // catch(Throwable t) {
        // LOGGER.log(Level.WARNING, "Error occured loading data access factories. " +
        // "Ignoring producer", t);
        // }
        // }

        return factories;
    }

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
