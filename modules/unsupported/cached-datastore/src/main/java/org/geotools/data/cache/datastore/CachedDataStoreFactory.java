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

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.geotools.data.AbstractDataStoreFactory;
import org.geotools.data.DataAccess;
import org.geotools.data.DataAccessFactory;
import org.geotools.data.DataAccessFinder;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.cache.op.CachedOp;
import org.geotools.data.cache.op.CachedOpSPI;
import org.geotools.data.cache.utils.CachedOpSPIMapParam;
import org.geotools.data.cache.utils.MapParam;
import org.geotools.util.KVP;
import org.opengis.feature.Feature;
import org.opengis.feature.type.FeatureType;

/**
 * 
 * @source $URL$
 */
public class CachedDataStoreFactory extends AbstractDataStoreFactory implements DataStoreFactorySpi {

    public final static String STORE_NAME = "CachedDataStore";

    public final static String STORE_DESC = "Add a cache layer to the selected datastore (the cache can be a local datastore or a spring pluggable cache)";

    // public static final Param REPOSITORY_PARAM = new Param("repository", Repository.class,
    // "The repository that will provide the store intances", true, null, new KVP(Param.LEVEL,
    // "advanced"));

    public final static String NAME_KEY = "Name";

    public static final Param NAME = new Param(NAME_KEY, String.class, "Name", true, "topp",
            new KVP(Param.ELEMENT, String.class));

    public final static String NAMESPACE_KEY = "Namespace";

    public static final Param NAMESPACE = new Param(NAMESPACE_KEY, String.class,
            "Namespace prefix", true, "topp", new KVP(Param.ELEMENT, String.class));

    public static final String SOURCE_TYPE_KEY = "SourceNameType";

    public static final Param SOURCE_TYPE = new Param(SOURCE_TYPE_KEY, String.class,
            "Setup a storage type for the source (use cache for spring cache)", true, null);

    public static final String SOURCE_PARAMS_KEY = "SourceParams";

    public static final Param SOURCE_PARAMS = new MapParam(SOURCE_PARAMS_KEY, Map.class,
            "The target data store to cache", true, null, new KVP(Param.ELEMENT, Param.class));

    public static final String CACHE_TYPE_KEY = "CacheNameType";

    public static final Param CACHE_TYPE = new Param(CACHE_TYPE_KEY, String.class,
            "Setup the cache type (use cache for spring cache)", true, null);

    public static final String CACHE_PARAMS_KEY = "CacheParams";

    public static final Param CACHE_PARAMS = new MapParam(CACHE_PARAMS_KEY, Map.class,
            "The target data store to cache", true, null, new KVP(Param.ELEMENT, Param.class));
    
    public static final String CACHEDOPSPI_PARAMS_KEY = "CachedOpParams";
    
    public static final Param CACHEDOPSPI_PARAMS = new CachedOpSPIMapParam(CACHEDOPSPI_PARAMS_KEY, Map.class,
            "The Map of SPI to use for Cached Operations", true, null, new KVP(Param.ELEMENT, Param.class));

    // public static final String CACHE_MANAGER_KEY = "CacheManager";

    // public static final Param CACHE_MANAGER = new MapParam(CACHE_MANAGER_KEY, Map.class,
    // "The set of cache managers to use for this store", false, new KVP(Param.ELEMENT,
    // Map.class));

    // public static final Param STORE = new Param("Store", String.class,
    // "The target data store to cache", true, null, new KVP(Param.ELEMENT, String.class));

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
        return new Param[] { NAME, NAMESPACE, SOURCE_TYPE, SOURCE_PARAMS, CACHE_TYPE, CACHE_PARAMS, CACHEDOPSPI_PARAMS };
    }

    public boolean isAvailable() {
        return true;
    }

    public CachedDataStoreFactory() {
        super();
    }

    public DataStore createDataStore(Map<String, Serializable> params) throws IOException {
        // Repository repository = lookup(REPOSITORY_PARAM, params, Repository.class);

        // DataStore source = repository.dataStore(new NameImpl(namespace, store));

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

        final String cacheType = lookup(CACHE_TYPE, params, String.class);
        final Map<String, Serializable> cacheParams = lookup(CACHE_PARAMS, params, Map.class);

        final DataStore cache = (DataStore) getDataStore(cacheParams, cacheType);
        
        final CacheManager cacheManager = new CacheManager(source, cache, createDataStoreUID(params));
        
        final Map<String, CachedOpSPI<CachedOp<?, ?>>> spiParams = lookup(CACHEDOPSPI_PARAMS, params, Map.class);
        
        cacheManager.load(spiParams.values());
        

//        if (cache == null) {
//            CachedOpSPI<?> spi = new STRFeatureSourceOpSPI();
//            props.putCachedOp(spi,
//                    spi.create(source, null, props, props.createCachedOpUID(spi.getOp())));
//            // cache = new SpringCachedDataStore(source);
//            // manager = new CacheManagerOld<DataStore>(cache);
//            // manager.setAllPolicy(CacheOp.values(), true);
//            return new CachedDataStore(props);
//        }

        return new CachedDataStore(cacheManager);
    }

    public static String createDataStoreUID(Map<String, Serializable> params) throws IOException {
        return new StringBuilder(lookup(NAMESPACE, params, String.class)).append(':').append(
                lookup(NAME, params, String.class)).toString();
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
    private static <T> T lookup(Param param, Map<String, Serializable> params, Class<T> target)
            throws IOException {
        T result = (T) param.lookUp(params);
        if (result == null) {
            return (T) param.getDefaultValue();
        } else {
            return result;
        }

    }

}
