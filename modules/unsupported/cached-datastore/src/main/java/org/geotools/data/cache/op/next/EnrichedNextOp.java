package org.geotools.data.cache.op.next;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.logging.Logger;

import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.CachedOpStatus;
import org.geotools.data.cache.op.schema.EnrichedSchemaOp;
import org.opengis.feature.simple.SimpleFeature;

public class EnrichedNextOp extends NextOp {

    public EnrichedNextOp(CacheManager cacheManager, final CachedOpStatus<SimpleFeature> status)
            throws IOException {
        super(cacheManager, status);
    }

    @Override
    public SimpleFeature updateCache(SimpleFeature sf) throws IOException {

        enrich(sf, false, LOGGER);

        return sf;

    }

    @Override
    public SimpleFeature getCache(SimpleFeature o) throws IOException {
        return updateCache(o);
    }

    private static void enrich(SimpleFeature sourceF, boolean updateTimestamp, Logger LOGGER)
            throws IOException {

        final Long hints = (Long) sourceF.getAttribute(EnrichedSchemaOp.HINTS_NAME);
        if (hints != null) {
            sourceF.setAttribute(EnrichedSchemaOp.HINTS_NAME, hints + 1);
        } else {
            sourceF.setAttribute(EnrichedSchemaOp.HINTS_NAME, 0L);
        }

        final Timestamp timestamp = (Timestamp) sourceF
                .getAttribute(EnrichedSchemaOp.TIMESTAMP_NAME);
        if (timestamp != null) {
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
