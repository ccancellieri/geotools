package org.geotools.data.cache.op;

import org.opengis.feature.simple.SimpleFeatureType;

public abstract class BaseSchemaOp<K> extends BaseOp<SimpleFeatureType, K> {

    public BaseSchemaOp(CacheManager cacheManager, final String uniqueName) {
        super(cacheManager, uniqueName);
    }

//    public abstract Collection<Property> enrich(Feature sourceF,Feature destinationF) throws IOException;

}
