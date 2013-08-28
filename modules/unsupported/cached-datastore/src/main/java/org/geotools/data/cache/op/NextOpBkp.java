package org.geotools.data.cache.op;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.feature.simple.SimpleSchema;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

public class NextOpBkp extends BaseOp<SimpleFeature, Object, Object> {

    private final transient Logger LOGGER = org.geotools.util.logging.Logging.getLogger(getClass()
            .getPackage().getName());

    private transient FeatureReader<SimpleFeatureType, SimpleFeature> fr = null;

    private transient FeatureWriter<SimpleFeatureType, SimpleFeature> fw = null;

    public NextOpBkp(CacheManager cacheManager, final String uniqueName) {
        super(cacheManager, uniqueName);
    }

    @Override
    public SimpleFeature updateCache(Object o) throws IOException {
        return getCache(o);
    }

    @Override
    public SimpleFeature getCache(Object o) throws IOException {
        verify(o);

        if (fr == null || fw == null) {
            throw new IOException("Unable to run next() action using a null reader or null writer");
        }

        // consider turning all geometries into packed ones, to save space
        final SimpleFeature sf = fr.next();
        final SimpleFeature df = fw.next();

        for (final Property p : enrich(sf, df, false, LOGGER)) {
            df.getProperty(p.getName()).setValue(p.getValue());
        }
        fw.write();
        return df;

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
                if (c.isAssignableFrom(SimpleSchema.LONG.getBinding())) {
                    Long oldValue = (Long) SimpleSchema.LONG.getBinding().cast(p.getValue());
                    p.setValue(oldValue != null ? ++oldValue : 0);
                } else {
                    throw new IOException("Unable to enrich this feature: wrong binding class ("
                            + c + ") for property: " + p.getName());
                }
            } else if (p.getName().getLocalPart().equalsIgnoreCase(SchemaOp.TIMESTAMP_NAME)) {
                final Class c = p.getType().getBinding();
                if (c.isAssignableFrom(SimpleSchema.DATETIME.getBinding())) {
                    final Timestamp oldValue = (Timestamp) SimpleSchema.DATETIME.getBinding().cast(p.getValue());
                    p.setValue(oldValue!=null?oldValue:new Timestamp(Calendar.getInstance().getTimeInMillis()));
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
    public void setCached(boolean isCached, Object key) {
        // do nothing
    };

    @Override
    public boolean isCached(Object o) {
        return true;
    }

    public FeatureReader<SimpleFeatureType, SimpleFeature> getFr() {
        return fr;
    }

    public void setFr(FeatureReader<SimpleFeatureType, SimpleFeature> fr) {
        this.fr = fr;
    }

    public FeatureWriter<SimpleFeatureType, SimpleFeature> getFw() {
        return fw;
    }

    public void setFw(FeatureWriter<SimpleFeatureType, SimpleFeature> fw) {
        this.fw = fw;
    }

}
