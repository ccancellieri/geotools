package org.geotools.data.cache.utils;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.cache.op.BaseFeatureSourceOp;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.store.ContentEntry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

import com.vividsolutions.jts.geom.Geometry;

public class PipelinedContentFeatureReader extends DelegateSimpleFeature implements
        FeatureReader<SimpleFeatureType, SimpleFeature> {

    private final ContentEntry entry;

    private final Transaction transaction;

    private FeatureWriter<SimpleFeatureType, SimpleFeature> fw = null;

    private FeatureReader<SimpleFeatureType, SimpleFeature> fr = null;

    private SimpleFeatureUpdaterReader fwDiff = null;

    private BaseFeatureSourceOp<?> featureSourceOp = null;

    private Query query = null;

    public PipelinedContentFeatureReader(ContentEntry entry, final Query query,
            final CacheManager cacheManager, final BaseFeatureSourceOp<Query> featureSourceOp)
            throws IOException {
        this(entry, query, cacheManager, featureSourceOp, Transaction.AUTO_COMMIT);
    }

    public PipelinedContentFeatureReader(ContentEntry entry, final Query query,
            final CacheManager cacheManager, final BaseFeatureSourceOp<?> featureSourceOp,
            final Transaction transaction) throws IOException {
        super(cacheManager);
        this.entry = entry;
        this.query = query;
        this.featureSourceOp = featureSourceOp;
        this.transaction = transaction;
    }

    @Override
    protected Name getFeatureTypeName() {
        return entry.getName();
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
            if (!fwDiff.hasNext()) {
                throw new IOException("missing feature in cache, this may never happen");
            }
            df = fwDiff.next();
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
        featureSourceOp.setCached((Geometry) df.getDefaultGeometry(), true);
        featureSourceOp.setDirty(query, false); // TODO
        return df;

    }

    @Override
    public boolean hasNext() throws IOException {
        if (fr == null || fwDiff == null) {
            final Query cacheQuery = featureSourceOp.queryCachedAreas(query);
            final Query sourceQuery = featureSourceOp.querySource(query);
            if (fr == null) {

                fr = cacheManager.getSource().getFeatureReader(sourceQuery, transaction);

                fw = cacheManager.getCache().getFeatureWriter(getFeatureTypeName().getLocalPart(),
                        sourceQuery.getFilter(), transaction);

            }
            if (fwDiff == null) {
                fwDiff = new SimpleFeatureUpdaterReader(entry, cacheQuery, cacheManager,
                        transaction);
            }
        }
        boolean notEnd = fr.hasNext() || fwDiff.hasNext();
        // if (!notEnd){
        // featureSourceOp.setCached(query, true); //TODO
        // featureSourceOp.setDirty(query, false); //TODO
        // }
        return notEnd;
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
        if (fwDiff != null) {
            try {
                fwDiff.close();
            } catch (IOException e) {
            }
        }
    }

}
