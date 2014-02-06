package org.geotools.data.cache.op;

public enum Operation {

    // ContentFeatureSource
    bounds, count, featureCollection, writer, schema,

    // DataStore
    typeNames, featureSource,

    // FeatureReader
    featureReader, featureType, hasNext, next;

}
