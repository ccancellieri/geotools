package org.geotools.data.cache.op;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.feature.simple.SimpleSchema;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.Name;
import org.opengis.filter.identity.FeatureId;

public class NextOp extends BaseOp<SimpleFeature, FeatureId> {

    private final transient Logger LOGGER = org.geotools.util.logging.Logging.getLogger(getClass()
            .getPackage().getName());

    // source feature
    private transient SimpleFeature sf = null;

    // destination feature
    private transient SimpleFeature df = null;

    public NextOp(CacheManager cacheManager, final String uniqueName) {
        super(cacheManager, uniqueName);
    }

    @Override
    public SimpleFeature updateCache(FeatureId o) throws IOException {
        verify(o);

        if (sf == null || df == null) {
            throw new IOException("Unable to run next() action using a null reader or null writer");
        }

        // consider turning all geometries into packed ones, to save space
        for (final Property p : enrich(sf, df, false, LOGGER)) {
            df.getProperty(p.getName()).setValue(p.getValue());
        }

        return df;

    }

    @Override
    public SimpleFeature getCache(FeatureId o) throws IOException {
        return updateCache(o);
    }

    private static Collection<Property> enrich(SimpleFeature sourceF, SimpleFeature destinationF,
            boolean updateTimestamp, Logger LOGGER) throws IOException {

        final Collection<Property> props = destinationF.getProperties();
        final Map<Name, Property> sourceMap = new HashMap<Name, Property>();
        for (Property p : sourceF.getValue()) {
            sourceMap.put(p.getName(), p);
        }
        for (Property p : props) {
            if (p.getName().getLocalPart().equalsIgnoreCase(SchemaOp.HINTS_NAME)) {
                final Class c = p.getType().getBinding();
                if (SimpleSchema.LONG.getBinding().isAssignableFrom(c)) {
                    final Object o = p.getValue();
                    if (o != null) {
                        final Long oldValue = (Long) SimpleSchema.LONG.getBinding().cast(o);
                        p.setValue(oldValue + 1);
                    } else {
                        p.setValue(0L);
                    }
                } else {
                    throw new IOException("Unable to enrich this feature: wrong binding class ("
                            + c + ") for property: " + p.getName());
                }
            } else if (p.getName().getLocalPart().equalsIgnoreCase(SchemaOp.TIMESTAMP_NAME)) {
                final Class c = p.getType().getBinding();
                if (SimpleSchema.DATETIME.getBinding().isAssignableFrom(c)) {
                    final Object o = p.getValue();
                    if (o != null) {
                        final Timestamp oldValue = (Timestamp) SimpleSchema.DATETIME.getBinding()
                                .cast(o);
                        p.setValue(oldValue);
                    } else {
                        p.setValue(new Timestamp(Calendar.getInstance().getTimeInMillis()));
                    }
                } else if (Date.class.isAssignableFrom(c)) {
                    final Object o = p.getValue();
                    if (o != null) {
                        Date date = (Date) o;
                        final Timestamp oldValue = new Timestamp(date.getTime());
                        p.setValue(oldValue);
                    } else {
                        p.setValue(new Timestamp(Calendar.getInstance().getTimeInMillis()));
                    }
                } else {
                    throw new IOException("Unable to enrich this feature: wrong binding class ("
                            + c + ") for property: " + p.getName());
                }
            } else {
                final Property newP = sourceMap.get(p.getName());
                if (newP != null) {
                    p.setValue(newP.getValue());
                } else {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Skipping not found property named: " + p.getName());
                    }
                }
            }
        }
        return props;
    }

    @Override
    public void setCached(FeatureId key, boolean isCached) {
        // do nothing
    };

    @Override
    public boolean isCached(FeatureId o) throws IOException {
        return true;
    }

    public SimpleFeature getSf() {
        return sf;
    }

    public void setSf(SimpleFeature sf) {
        this.sf = sf;
    }

    public SimpleFeature getDf() {
        return df;
    }

    public void setDf(SimpleFeature df) {
        this.df = df;
    }

    @Override
    public boolean isDirty(FeatureId key) throws IOException {
        return false;
    }

    @Override
    public void setDirty(FeatureId query) throws IOException {
        throw new UnsupportedOperationException();
    }

}
