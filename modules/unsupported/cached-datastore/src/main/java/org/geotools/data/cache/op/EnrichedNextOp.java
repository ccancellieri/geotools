package org.geotools.data.cache.op;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.logging.Logger;

import org.geotools.feature.simple.SimpleSchema;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;

public class EnrichedNextOp extends NextOp {

    public EnrichedNextOp(CacheManager cacheManager, final String uniqueName) throws IOException {
        super(cacheManager, uniqueName);
    }

    @Override
    public SimpleFeature updateCache(SimpleFeature sf) throws IOException {
        verify(sf);

        // consider turning all geometries into packed ones, to save space
        for (final Property p : enrich(sf, false, LOGGER)) {
            sf.getProperty(p.getName()).setValue(p.getValue());
        }

        return sf;

    }

    @Override
    public SimpleFeature getCache(SimpleFeature o) throws IOException {
        return updateCache(o);
    }

    private static Collection<Property> enrich(SimpleFeature sourceF, boolean updateTimestamp,
            Logger LOGGER) throws IOException {

        final Collection<Property> props = sourceF.getProperties();

        final Property hints = sourceF.getProperty(EnrichedSchemaOp.HINTS_NAME);
        if (hints != null) {
            final Class c = hints.getType().getBinding();
            final Object o = hints.getValue();
            if (o != null) {
                if (SimpleSchema.LONG.getBinding().isAssignableFrom(c)) {
                    final Long oldValue = (Long) SimpleSchema.LONG.getBinding().cast(o);
                    hints.setValue(oldValue + 1);
                } else {
                    throw new IOException("Unable to enrich this feature: wrong binding class ("
                            + c + ") for property: " + hints.getName());
                }
            } else {
                hints.setValue(0L);
            }
        }

        final Property timestamp = sourceF.getProperty(EnrichedSchemaOp.TIMESTAMP_NAME);
        if (timestamp != null) {
            final Class c = timestamp.getType().getBinding();
            final Object o = timestamp.getValue();
            if (updateTimestamp || o == null) {
                timestamp.setValue(new Timestamp(Calendar.getInstance().getTimeInMillis()));
            } else if (SimpleSchema.DATETIME.getBinding().isAssignableFrom(c)) {
                final Timestamp oldValue = (Timestamp) SimpleSchema.DATETIME.getBinding().cast(o);
                timestamp.setValue(oldValue);
            } else if (Date.class.isAssignableFrom(c)) {
                Date date = (Date) o;
                final Timestamp oldValue = new Timestamp(date.getTime());
                timestamp.setValue(oldValue);
            } else {
                throw new IOException("Unable to enrich this feature: wrong binding class (" + c
                        + ") for property: " + timestamp.getName());
            }
        }
        return props;
    }

}
