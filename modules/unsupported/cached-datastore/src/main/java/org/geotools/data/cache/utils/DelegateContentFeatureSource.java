package org.geotools.data.cache.utils;

import java.io.IOException;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.BaseOp;
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

    private final BaseFeatureOp<Integer> countOp;

    private final BaseOp<Query, ReferencedEnvelope> boundsOp;

    // private final BaseFeatureOpStatus status;

    // private final boolean sharedStatus;

    /**
     * 
     * @param cacheManager
     * @param query
     * @param status
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public DelegateContentFeatureSource(final CacheManager cacheManager, final Query query,
            final BaseFeatureOpStatus status) throws IllegalArgumentException, IOException {
        this(cacheManager, query, status.getEntry());// , status, true);
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
        super(entry, query);
        if (cacheManager == null)
            throw new IllegalArgumentException(
                    "Unable to initialize a cache manager !=null is needed");

        // schema
        this.schemaOp = cacheManager.getCachedOpOfType(Operation.schema, SchemaOp.class);

        this.cacheManager = cacheManager;

        // status
        // this.status = status;
        // shared
        // this.sharedStatus = sharedStatus;

        // featureSource
        this.featureSourceOp = cacheManager.getCachedOpOfType(Operation.featureSource,
                BaseFeatureOp.class);
        // featureReader
        this.featureReaderOp = cacheManager.getCachedOpOfType(Operation.featureReader,
                BaseFeatureOp.class);
        // count
        this.countOp = cacheManager.getCachedOpOfType(Operation.count, BaseFeatureOp.class);
        // bounds
        this.boundsOp = cacheManager.getCachedOpOfType(Operation.bounds, BaseOp.class); // TODO
    }

    /**
     * Initialize shared status for feature related operation
     */
    protected void init() {
        // if (sharedStatus) {
        // // featureSource
        // if (featureSourceOp != null) {
        // featureSourceOp.setStatus(this.status);
        // }
        // // featureReader
        // if (featureSourceOp != null) {
        // featureSourceOp.setStatus(this.status);
        // }
        // // count
        // if (countOp != null) {
        // countOp.setStatus(this.status);
        // }
        // }
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        ReferencedEnvelope env = null;
        if (boundsOp != null) {
            try {
                if (!boundsOp.isCached(query) || boundsOp.isDirty(query)) {
                    env = boundsOp.updateCache(query);
                } else {
                    env = boundsOp.getCache(query);
                }
            } catch (IOException e) {
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
        if (countOp != null) {
            try {
                if (!countOp.isCached(query) || countOp.isDirty(query)) {
                    count = countOp.updateCache(query);
                } else {
                    count = countOp.getCache(query);
                }
            } catch (IOException e) {
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
            Query q = new Query(query.getTypeName(), query.getFilter(),
                    Collections.singletonList(query.getProperties().get(0)));
            FeatureReader<SimpleFeatureType, SimpleFeature> reader = cacheManager.getSource()
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
        if (featureSourceOp != null) {
            try {
                if (!featureSourceOp.isCached(query) || featureSourceOp.isDirty(query)) {

                    final SimpleFeatureSource source = featureSourceOp.updateCache(query);
                    if (source != null) {
                        return new SimpleFeatureCollectionReader(cacheManager, getAbsoluteSchema(),
                                source.getFeatures(query));
                    } else {
                        featureSourceOp.setDirty(query, true);
                        throw new IOException(
                                "Unable to create a simple feature source from the passed query: "
                                        + query);
                    }
                }
                if (featureReaderOp != null) {
                    if (!featureReaderOp.isCached(query) || featureReaderOp.isDirty(query)) {
                        return featureReaderOp.updateCache(query);
                    } else {
                        featureReaderOp.getCache(query);
                    }
                }
            } catch (IOException e) {
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.log(Level.SEVERE, e.getLocalizedMessage(), e);
                }
                throw e;
            }
        }
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.log(Level.WARNING, "No cached operation is found: " + Operation.featureSource);
        }
        return new DelegateSimpleFeatureReader(cacheManager, cacheManager.getSource()
                .getFeatureReader(query, transaction), getSchema());
        // return new SimpleFeatureCollectionReader(cacheManager, getSchema(),
        // delegate.getFeatures(query));
    }

    @Override
    protected SimpleFeatureType buildFeatureType() throws IOException {
        SimpleFeatureType schema = null;
        if (schemaOp != null) {
            final Name name = getEntry().getName();
            try {
                if (!schemaOp.isCached(name) || schemaOp.isDirty(name)) {
                    schema = schemaOp.updateCache(name);
                } else {
                    schema = schemaOp.getCache(name);
                }
            } catch (IOException e) {
                schemaOp.setDirty(name, true);
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.log(Level.SEVERE, e.getLocalizedMessage(), e);
                }
            }
        }
        if (schema != null) {
            return schema;
        } else {
            return cacheManager.getSource().getSchema(entry.getTypeName());
        }
    }

}
