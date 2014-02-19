package org.geotools.data.cache.op.feature.bounds;

import java.io.IOException;

import org.geotools.data.Query;
import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.feature.BaseFeatureOp;
import org.geotools.data.cache.op.feature.BaseFeatureOpStatus;
import org.geotools.data.cache.utils.DelegateContentFeatureSource;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;

import com.vividsolutions.jts.geom.Envelope;

public class FeatureBoundsOp extends BaseFeatureOp<ReferencedEnvelope> {

    public FeatureBoundsOp(final CacheManager cacheManager, final BaseFeatureOpStatus status)
            throws IOException {
        super(cacheManager, status);
    }

    @Override
    public ReferencedEnvelope getCache(Query query) throws IOException {
        // if we are here the cache covers the requested envelope
        final Envelope env = BaseFeatureOpStatus.getEnvelope(query.getFilter());
        return new ReferencedEnvelope(env, query.getCoordinateSystemReproject());
    }

    @Override
    public ReferencedEnvelope updateCache(Query query) throws IOException {

        final SimpleFeatureSource featureSource = new DelegateContentFeatureSource(cacheManager,
                query, getStatus());
//        {
//            @Override
//            protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
//                    throws IOException {
//
//                return new PipelinedContentFeatureReader(getStatus(), query, cacheManager,
//                        Transaction.AUTO_COMMIT);
//            }
//        };

        return featureSource.getFeatures(query).getBounds();
    }
}
