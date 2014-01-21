package org.geotools.data.cache.op;

import java.io.IOException;

import org.opengis.feature.simple.SimpleFeatureType;

public abstract class BaseSchemaOp<K> extends BaseOp<SimpleFeatureType, K> {

    public BaseSchemaOp(CacheManager cacheManager, final String uniqueName) throws IOException {
        super(cacheManager, uniqueName);
    }

}
