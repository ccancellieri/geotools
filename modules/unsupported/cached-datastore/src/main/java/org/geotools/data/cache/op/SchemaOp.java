package org.geotools.data.cache.op;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Calendar;

import org.geotools.feature.NameImpl;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.feature.simple.SimpleSchema;
import org.geotools.feature.type.AttributeDescriptorImpl;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

public class SchemaOp extends BaseSchemaOp<Name, String> {

    // final Map<Name,SimpleFeatureType> schemas=new HashMap<Name, SimpleFeatureType>();

    public SchemaOp(CacheManager cacheManager, final String uniqueName) {
        super(cacheManager, uniqueName);
    }

    public static String TIMESTAMP_NAME = "TimeStamp";

    public static String HINTS_NAME = "Hints";

    @Override
    public SimpleFeatureType updateCache(Name name) throws IOException {

        verify(name);

        final SimpleFeatureTypeBuilder b = new SimpleFeatureTypeBuilder();

        SimpleFeatureType schema = cacheManager.getSource().getSchema(name);

        // initialize the builder
        b.init(schema);

        // add TIMESTAMP
        b.add(new AttributeDescriptorImpl(SimpleSchema.DATETIME, new NameImpl(TIMESTAMP_NAME), 1,
                1, false, new Timestamp(Calendar.getInstance().getTimeInMillis())));

        // cache hits
        b.add(new AttributeDescriptorImpl(SimpleSchema.LONG, new NameImpl(HINTS_NAME), 1, 1, false,
                0));

        final SimpleFeatureType newSchema = b.buildFeatureType();

        if (isCached(cacheManager.getUID())) {
            // cache.createSchema(SimpleFeatureTypeBuilder.copy(store.getSchema(name)));
            cacheManager.getCache().updateSchema(name, newSchema);
        } else {
            cacheManager.getCache().createSchema(newSchema);
        }

        return newSchema;
    }

    @Override
    public SimpleFeatureType getCache(Name o) throws IOException {
        verify(o);
        return cacheManager.getCache().getSchema(o);
    }
    //
    // @Override
    // public Collection<Property> enrich(Feature sourceF, Feature destinationF) throws IOException {
    // final Collection<Property> props = destinationF.getProperties();
    // final Map<Name, Property> sourceMap = new HashMap<Name, Property>();
    // for (Property p : sourceF.getValue()) {
    // sourceMap.put(p.getName(), p);
    // }
    // for (Property p : props) {
    // if (p.getName().getLocalPart().equalsIgnoreCase(HINTS_NAME)) {
    // final Class c = p.getType().getBinding();
    // if (c.isAssignableFrom(SimpleSchema.LONG.getBinding())) {
    // Long oldValue = (Long) SimpleSchema.LONG.getBinding().cast(p.getValue());
    // p.setValue(oldValue != null ? ++oldValue : 0);
    // } else {
    // throw new IOException("Unable to enrich this feature: wrong binding class ("
    // + c + ") for property: " + p.getName());
    // }
    // } else if (p.getName().getLocalPart().equalsIgnoreCase(TIMESTAMP_NAME)) {
    // final Class c = p.getType().getBinding();
    // if (c.isAssignableFrom(SimpleSchema.DATETIME.getBinding())) {
    // p.setValue(new Timestamp(Calendar.getInstance().getTimeInMillis()));
    // } else {
    // throw new IOException("Unable to enrich this feature: wrong binding class ("
    // + c + ") for property: " + p.getName());
    // }
    // } else {
    // final Property newP = sourceMap.get(p.getName());
    // if (newP != null) {
    // p.setValue(newP.getValue());
    // } else {
    // if (LOGGER.isLoggable(Level.WARNING)) {
    // LOGGER.warning("Skipping not found property named: " + p.getName());
    // }
    // }
    // }
    // }
    // return props;
    // }

}
