package org.geotools.data.cache.op.next;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.logging.Logger;

import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.CachedOpStatus;
import org.geotools.data.cache.op.schema.EnrichedSchemaOp;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class EnrichedNextOp extends NextOp {

    public EnrichedNextOp(CacheManager cacheManager, final CachedOpStatus<SimpleFeature> status)
            throws IOException {
        super(cacheManager, status);
    }

    @Override
    public SimpleFeature updateCache(SimpleFeature sf) throws IOException {
        // if (schema!=null){
        // sf=SimpleFeatureBuilder.build(schema, sf.getAttributes(), sf.getID());
        // }
        enrich(sf, sf.getFeatureType(), false, LOGGER);
        return sf;

    }

    @Override
    public SimpleFeature getCache(SimpleFeature o) throws IOException {
        return updateCache(o);
    }

    private static void enrich(SimpleFeature sourceF, final SimpleFeatureType schema,
            boolean updateTimestamp, Logger LOGGER) throws IOException {
        if (schema.indexOf(EnrichedSchemaOp.HINTS_NAME) > -1) {
            final Long hints = (Long) sourceF.getAttribute(EnrichedSchemaOp.HINTS_NAME);
            if (hints != null) {
                sourceF.setAttribute(EnrichedSchemaOp.HINTS_NAME, hints + 1);
            } else {
                sourceF.setAttribute(EnrichedSchemaOp.HINTS_NAME, 0L);
            }
        }
        if (schema.indexOf(EnrichedSchemaOp.TIMESTAMP_NAME) > -1) {

            final Object obj = sourceF.getAttribute(EnrichedSchemaOp.TIMESTAMP_NAME);
            if (obj != null) {
//                Timestamp timestamp=null;
//                if (Timestamp.class.isAssignableFrom(obj.getClass())) {
//                    timestamp = (Timestamp) obj;
//                } else if (Date.class.isAssignableFrom(obj.getClass())){
//                    Date date=(Date)obj;
//                    timestamp = new Timestamp(date.getTime());
//                } else {
//                    throw new IllegalArgumentException("Unrecognized type for attribute: "+EnrichedSchemaOp.TIMESTAMP_NAME);
//                }
                if (updateTimestamp) {
                    sourceF.setAttribute(EnrichedSchemaOp.TIMESTAMP_NAME, new Timestamp(Calendar
                            .getInstance().getTimeInMillis()));
                }
            } else {
                sourceF.setAttribute(EnrichedSchemaOp.TIMESTAMP_NAME, new Timestamp(Calendar
                        .getInstance().getTimeInMillis()));
            }
        }
    }

}
