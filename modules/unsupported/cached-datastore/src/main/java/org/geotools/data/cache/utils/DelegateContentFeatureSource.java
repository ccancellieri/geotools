package org.geotools.data.cache.utils;

import java.io.IOException;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.cache.op.feature.BaseFeatureOp;
import org.geotools.data.cache.op.feature.BaseFeatureOpStatus;
import org.geotools.data.cache.op.schema.SchemaOp;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.geom.Geometry;

/**
 * Use CacheManager to try cached operations if no operation is defined then delegates to the CachedOps found into the passed {@link CacheManager}
 * 
 * @author carlo cancellieri - GeoSolutions SAS
 * 
 */
@SuppressWarnings("unchecked")
public class DelegateContentFeatureSource extends ContentFeatureSource {

    private final transient Logger LOGGER = org.geotools.util.logging.Logging.getLogger(getClass()
            .getPackage().getName());

    protected final CacheManager cacheManager;

    private final SchemaOp schemaOp;

    private final BaseFeatureOp<SimpleFeatureSource> featureSourceOp;

    private final BaseFeatureOp<FeatureReader<SimpleFeatureType, SimpleFeature>> featureReaderOp;

    private final BaseFeatureOp<Integer> featureCountOp;

    private final BaseFeatureOp<ReferencedEnvelope> featureBoundsOp;

    public DelegateContentFeatureSource(final DelegateContentFeatureSource source)
            throws IllegalArgumentException, IOException {
        super(source.entry, source.query);

        this.cacheManager = source.cacheManager;
        this.schemaOp = source.schemaOp;
        this.featureSourceOp = source.featureSourceOp;
        this.featureReaderOp = source.featureReaderOp;
        this.featureCountOp = source.featureCountOp;
        this.featureBoundsOp = source.featureBoundsOp;
    }

    /**
     * 
     * @param cacheManager
     * @param query
     * @param status
     * @param sharedStatus if true the passed status will be shared between BaseFeatureOp(erations).
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public DelegateContentFeatureSource(final CacheManager cacheManager, final Query query,
            final ContentEntry entry) throws IllegalArgumentException, IOException {
        this(cacheManager, query, entry, null, true);
    }

    public DelegateContentFeatureSource(final CacheManager cacheManager, final Query query,
            final BaseFeatureOpStatus featureStatus) throws IllegalArgumentException, IOException {
        super(featureStatus.getEntry(), query);

        if (cacheManager == null)
            throw new IllegalArgumentException(
                    "Unable to initialize cache manager !=null and featureStatus != null are needed");

        this.cacheManager = cacheManager;

        // schema
        this.schemaOp = cacheManager.getCachedOpOfType(Operation.schema, SchemaOp.class);

        this.schema = featureStatus.getSchema();

        // featureBounds
        this.featureBoundsOp = cacheManager.getCachedOpOfType(Operation.featureBounds,
                BaseFeatureOp.class);
        // featureSource
        this.featureSourceOp = cacheManager.getCachedOpOfType(Operation.featureSource,
                BaseFeatureOp.class);
        // featureReader
        this.featureReaderOp = cacheManager.getCachedOpOfType(Operation.featureReader,
                BaseFeatureOp.class);
        // featureCount
        this.featureCountOp = cacheManager.getCachedOpOfType(Operation.featureCount,
                BaseFeatureOp.class);

        // try to use the same status for all the feature related operations
        if (this.featureBoundsOp != null) {
            // featureBounds
            copyFeatureStatus(featureStatus, this.featureBoundsOp.getStatus());
        } else if (this.featureReaderOp != null) {
            // featureReader
            copyFeatureStatus(featureStatus, this.featureReaderOp.getStatus());
        } else if (this.featureSourceOp != null) {
            // featureSource
            copyFeatureStatus(featureStatus, this.featureSourceOp.getStatus());
        } else if (this.featureCountOp != null) {
            // featureCount
            copyFeatureStatus(featureStatus, this.featureCountOp.getStatus());
        }
    }

    public DelegateContentFeatureSource(final CacheManager cacheManager, final Query query,
            final ContentEntry entry, final SimpleFeatureType schema,
            final boolean sharedFeatureStatus) throws IllegalArgumentException, IOException {
        super(entry, query);
        if (cacheManager == null)
            throw new IllegalArgumentException(
                    "Unable to initialize cache manager !=null and featureStatus != null are needed");

        this.cacheManager = cacheManager;

        // schema
        this.schemaOp = cacheManager.getCachedOpOfType(Operation.schema, SchemaOp.class);

        if (schema == null) {
            this.schema = getSchema();
        } else {
            this.schema = schema;
        }

        // featureBounds
        this.featureBoundsOp = cacheManager.getCachedOpOfType(Operation.featureBounds,
                BaseFeatureOp.class);
        // featureSource
        this.featureSourceOp = cacheManager.getCachedOpOfType(Operation.featureSource,
                BaseFeatureOp.class);
        // featureReader
        this.featureReaderOp = cacheManager.getCachedOpOfType(Operation.featureReader,
                BaseFeatureOp.class);
        // featureCount
        this.featureCountOp = cacheManager.getCachedOpOfType(Operation.featureCount,
                BaseFeatureOp.class);

        Geometry re = null;
        if (sharedFeatureStatus) {
            // try to use the same status for all the feature related operations
            BaseFeatureOpStatus featureStatus = null;
            // featureBounds
            if (this.featureBoundsOp != null) {
                featureStatus = this.featureBoundsOp.getStatus();
                re = initFeatureStatus(featureStatus, entry, schema, query, re);
            } else if (this.featureReaderOp != null) {
                // featureReader
                if (featureStatus == null) {
                    featureStatus = this.featureReaderOp.getStatus();
                    re = initFeatureStatus(featureStatus, entry, schema, query, re);
                }
            } else if (this.featureSourceOp != null) {
                // featureSource
                if (featureStatus == null) {
                    featureStatus = this.featureSourceOp.getStatus();
                    re = initFeatureStatus(featureStatus, entry, schema, query, re);
                }
            } else if (this.featureCountOp != null) {
                // featureCount
                if (featureStatus == null) {
                    featureStatus = this.featureCountOp.getStatus();
                    re = initFeatureStatus(featureStatus, entry, schema, query, re);
                }
            }
        } else {
            // featureBounds
            if (this.featureBoundsOp != null) {
                re = initFeatureStatus(this.featureBoundsOp.getStatus(), entry, schema, query, re);
            }
            // featureReader
            if (this.featureReaderOp != null) {
                re = initFeatureStatus(this.featureReaderOp.getStatus(), entry, schema, query, re);
            }
            // featureSource
            if (this.featureSourceOp != null) {
                re = initFeatureStatus(this.featureSourceOp.getStatus(), entry, schema, query, re);
            }
            // featureCount
            if (this.featureCountOp != null) {
                re = initFeatureStatus(this.featureCountOp.getStatus(), entry, schema, query, re);
            }
        }
    }

    private Geometry initFeatureStatus(BaseFeatureOpStatus status, ContentEntry entry,
            SimpleFeatureType schema, Query query, Geometry geom) throws IOException {
        status.setEntry(entry);
        status.setSchema(schema);
        if (geom == null) {
            MathTransform transform = null;
            if (query != null) {
                transform = status.getTransformation(query.getCoordinateSystemReproject());
            }
            geom = BaseFeatureOpStatus.getGeometry(getBounds(), transform);
        }
        status.setOriginalGeometry(geom);
        return geom;
    }

    private void copyFeatureStatus(BaseFeatureOpStatus source, BaseFeatureOpStatus destination) {
        destination.setEntry(source.getEntry());
        destination.setSchema(source.getSchema());
        destination.setOriginalGeometry(source.getOriginalGeometry());
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        ReferencedEnvelope env = null;
        if (featureBoundsOp != null) {
            try {
                if (featureBoundsOp.isFullyCached()) {
                    // the quick way
                    return cacheManager.getCache().getFeatureSource(entry.getTypeName())
                            .getBounds(query);
                }
                if (!featureBoundsOp.isCached(query) || featureBoundsOp.isDirty(query)) {
                    env = featureBoundsOp.updateCache(query);
                    featureBoundsOp.save();
                } else {
                    env = featureBoundsOp.getCache(query);
                }
            } catch (IOException e) {
                featureBoundsOp.setDirty(query, true);
                featureBoundsOp.save();
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.log(Level.SEVERE, e.getLocalizedMessage(), e);
                }
            }
        }
        if (env != null) {
            return env;
        } else {
            return cacheManager.getSource().getFeatureSource(entry.getTypeName()).getBounds(query);
        }
    }

    @Override
    protected int getCountInternal(Query query) throws IOException {
        Integer count = null;
        if (featureCountOp != null) {
            try {
                if (featureCountOp.isFullyCached()) {
                    // the quick way
                    return cacheManager.getCache().getFeatureSource(entry.getTypeName())
                            .getCount(query);
                }
                if (!featureCountOp.isCached(query) || featureCountOp.isDirty(query)) {
                    count = featureCountOp.updateCache(query);
                    featureCountOp.save();
                } else {
                    count = featureCountOp.getCache(query);
                }
            } catch (IOException e) {
                featureCountOp.setDirty(query, true);
                featureCountOp.save();
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.log(Level.SEVERE, e.getLocalizedMessage(), e);
                }
            }
        }
        if (count != null) {
            return count;
        } else {
            count = cacheManager.getSource().getFeatureSource(entry.getTypeName()).getCount(query);

            if (count != -1) {
                // optimization worked, return maxFeatures if count is
                // greater.
                int maxFeatures = query.getMaxFeatures();
                return (count < maxFeatures) ? count : maxFeatures;
            }

            // Okay lets count the FeatureReader
            count = 0;
            final Query q = new Query(query.getTypeName(), query.getFilter(),
                    Collections.singletonList(query.getProperties().get(0)));
            final FeatureReader<SimpleFeatureType, SimpleFeature> reader = cacheManager.getSource()
                    .getFeatureReader(q, Transaction.AUTO_COMMIT);
            try {
                for (; reader.hasNext(); count++) {
                    reader.next();
                }
            } finally {
                reader.close();
            }

            return count;
        }
    }

    @Override
    public boolean canRetype() {
        /**
         * overridden due to: java.lang.IllegalArgumentException: FeatureReader allready produces contents with the correct schema at
         * org.geotools.data.ReTypeFeatureReader.typeAttributes(ReTypeFeatureReader.java:126) at
         * org.geotools.data.ReTypeFeatureReader.<init>(ReTypeFeatureReader.java:100) at
         * org.geotools.data.store.ContentFeatureSource.getReader(ContentFeatureSource.java:590) at
         * org.geotools.data.store.ContentFeatureCollection.features(ContentFeatureCollection.java:238)
         */
        return true;
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
            throws IOException {
        if (featureReaderOp != null) {
            if (featureReaderOp.isFullyCached()) {
                // the quick way
                return cacheManager.getCache().getFeatureReader(query, transaction);
            }
            if (!featureReaderOp.isCached(query) || featureReaderOp.isDirty(query)) {
                return featureReaderOp.updateCache(query);
            } else {
                return featureReaderOp.getCache(query);
            }
        } else if (featureSourceOp != null) {
            final SimpleFeatureSource source;
            if (featureSourceOp.isFullyCached()) {
                // the quick way
                source = cacheManager.getCache().getFeatureSource(query.getTypeName());
            } else if (!featureSourceOp.isCached(query) || featureSourceOp.isDirty(query)) {
                source = featureSourceOp.updateCache(query);
            } else {
                source = featureSourceOp.getCache(query);
            }
            if (source != null) {
                return new SimpleFeatureCollectionReader(cacheManager, schema,
                        source.getFeatures(query));
            } else {
                featureSourceOp.setDirty(query, true);
                featureSourceOp.save();
                throw new IOException(
                        "Unable to create a simple feature source from the passed query: " + query);
            }
        }
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.log(Level.WARNING, "No cached operation is found: " + Operation.featureSource);
        }
        return new DelegateSimpleFeatureReader(cacheManager, cacheManager.getSource()
                .getFeatureReader(query, transaction), schema);
    }

    @Override
    protected SimpleFeatureType buildFeatureType() throws IOException {
        final Name name = getEntry().getName();
        SimpleFeatureType schema = null;
        if (schemaOp != null) {
            try {
                if (!schemaOp.isCached(name) || schemaOp.isDirty(name)) {
                    schema = schemaOp.updateCache(name);
                    schemaOp.save();
                } else {
                    schema = schemaOp.getCache(name);
                }
            } catch (IOException e) {
                schemaOp.setDirty(name, true);
                schemaOp.save();
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.log(Level.SEVERE, e.getLocalizedMessage(), e);
                }
            }
        }
        if (schema != null) {
            return schema;
        } else {
            return cacheManager.getSource().getSchema(name);
        }
    }

}
