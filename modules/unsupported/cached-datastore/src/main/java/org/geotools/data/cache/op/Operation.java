package org.geotools.data.cache.op;

public enum Operation {
    
    // ContentFeatureSource
    featureBounds, featureCount, featureCollection, writer, schema,

    // DataStore
    typeNames, featureSource,

    // FeatureReader
    featureReader, featureType, hasNext, next;

    
}
