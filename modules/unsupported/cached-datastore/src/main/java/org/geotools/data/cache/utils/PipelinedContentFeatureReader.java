package org.geotools.data.cache.utils;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.cache.op.feature.BaseFeatureOp;
import org.geotools.data.cache.op.feature.BaseFeatureOpStatus;
import org.geotools.data.simple.SimpleFeatureReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

public class PipelinedContentFeatureReader extends DelegateSimpleFeature implements
        FeatureReader<SimpleFeatureType, SimpleFeature> {

    // private final ContentEntry entry;

    private final Transaction transaction;

    private FeatureWriter<SimpleFeatureType, SimpleFeature> fw = null;

    private FeatureReader<SimpleFeatureType, SimpleFeature> fr = null;

    private FeatureReader<SimpleFeatureType, SimpleFeature> frDiff = null;

//    private final BaseFeatureOp<SimpleFeatureReader> featureReaderOp;

    private final BaseFeatureOpStatus status;

    private Query query = null;

    private Geometry queryGeom;

    public PipelinedContentFeatureReader(final BaseFeatureOpStatus status, final Query query,
            final CacheManager cacheManager) throws IOException {
        this(status, query, cacheManager, Transaction.AUTO_COMMIT);
    }

    public PipelinedContentFeatureReader(final BaseFeatureOpStatus status, final Query query,
            final CacheManager cacheManager, final Transaction transaction) throws IOException {
        super(cacheManager);

        this.status = status;
        this.query = query;
        // featureSource
        // this.featureSourceOp = cacheManager.getCachedOpOfType(Operation.featureSource,
        // BaseFeatureOp.class);
        // featureReader
//        this.featureReaderOp = cacheManager.getCachedOpOfType(Operation.featureReader,
//                BaseFeatureOp.class);

        this.transaction = transaction;
    }

    @Override
    protected Name getFeatureTypeName() {
        return status.getEntry().getName();
    }

    @Override
    protected SimpleFeature getNextInternal() throws IllegalArgumentException,
            NoSuchElementException, IOException {
        final SimpleFeature df;
        final SimpleFeature sf;
        if (fr.hasNext()) {
            if (!fw.hasNext()) {
                try {
                    if (fw != null) {
                        fw.close();
                    }
                } catch (IOException e) {
                }
                fw = cacheManager.getCache().getFeatureWriterAppend(
                        getFeatureTypeName().getLocalPart(), transaction);
            }

            df = fw.next();
            sf = fr.next();
            for (int i = 0; i < sf.getAttributeCount(); i++) {
                final Object a = sf.getAttribute(i);
                df.setAttribute(i, a);

            }
        } else { // if (frDiff.hasNext()) {
            if (!frDiff.hasNext()) {
                throw new IOException("missing feature in cache, this may never happen");
            }
            df = frDiff.next();
        }
        return df;
    }

    @Override
    public SimpleFeature next() throws IOException, IllegalArgumentException,
            NoSuchElementException {
        final SimpleFeature df;
        if (fr.hasNext()) {
            df = super.next();
            fw.write();
        } else {
            // conclude returning the diff
            df = super.next();
            // fwDiff.write();
        }

        // set as cached
        status.setCached((Geometry) df.getDefaultGeometry(), true);

        return df;

    }

    @Override
    public boolean hasNext() throws IOException {
        if (fr == null || frDiff == null) {
            Query cacheQuery = null;
            Query sourceQuery = null;
            try {
                // final Filter[] sF = BaseFeatureOpStatus.splitFilters(query, status.getSchema());
                final Envelope env = BaseFeatureOpStatus.getEnvelope(query.getFilter());
                MathTransform transform = status.getTransformation(query
                        .getCoordinateSystemReproject());
                queryGeom = BaseFeatureOpStatus.getGeometry(env, transform);
                final String geoName = status.getGeometryName();
                final String typeName = query.getTypeName();
                cacheQuery = status.queryAreas(typeName, geoName, queryGeom, transform, true, null);
                sourceQuery = status.queryAreas(typeName, geoName, queryGeom, transform, false,
                        query.getFilter());
                if (fr == null) {

                    fr = cacheManager.getSource().getFeatureReader(sourceQuery, transaction);

                    fw = cacheManager.getCache().getFeatureWriter(
                            getFeatureTypeName().getLocalPart(), sourceQuery.getFilter(),
                            transaction);

                }
                if (frDiff == null) {
//                    if (featureReaderOp != null) {
//                        if (!featureReaderOp.isCached(query) || featureReaderOp.isDirty(query)) {
//                            frDiff = featureReaderOp.updateCache(query);
//                        } else {
//                            frDiff = featureReaderOp.getCache(query);
//                        }
//
//                    } else {
                        frDiff = new DelegateSimpleFeatureReader(cacheManager, cacheManager
                                .getCache().getFeatureReader(cacheQuery, transaction),
                                status.getSchema());
//                    }

                }
            } catch (IOException e) {
                LOGGER.severe(e.getLocalizedMessage());
                return false;
            }

        }
        boolean hasNext = fr.hasNext() || frDiff.hasNext();
        if (!hasNext) {
            if (queryGeom != null) {
                // set query as cached
                status.setCached(queryGeom, true);
                // save the status
//                featureReaderOp.save();
                // TODO free the locks
            }
        }
        return hasNext;
    }

    @Override
    public void close() throws IOException {
        if (fr != null) {
            try {
                fr.close();
            } catch (IOException e) {
            }
        }
        if (fw != null) {
            try {
                fw.close();
            } catch (IOException e) {
            }
        }
        if (frDiff != null) {
            try {
                frDiff.close();
            } catch (IOException e) {
            }
        }
        cacheManager.save();
    }

}
