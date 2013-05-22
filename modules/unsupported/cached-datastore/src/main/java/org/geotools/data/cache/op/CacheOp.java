package org.geotools.data.cache.op;

public enum CacheOp {

    // ContentFeatureSource
    bounds, count, featureCollection, writer, schema,

    // DataStore
    typeNames, featureSource,

    // FeatureReader
    featureType, hasNext, next;

}
