package org.geotools.data.cache.utils;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.store.ContentEntry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

public class SimpleFeatureUpdaterReader extends DelegateSimpleFeature {

    private final ContentEntry entry;

    private final Transaction transaction;

    private FeatureWriter<SimpleFeatureType, SimpleFeature> fw = null;

    public SimpleFeatureUpdaterReader(ContentEntry entry, final Query query,
            final CacheManager cacheManager) throws IOException {
        this(entry, query, cacheManager, Transaction.AUTO_COMMIT);
    }

    public SimpleFeatureUpdaterReader(ContentEntry entry, final Query query,
            final CacheManager cacheManager, final Transaction transaction) throws IOException {

        super(cacheManager);

        this.entry = entry;

        this.transaction = transaction;

        fw = cacheManager.getCache().getFeatureWriter(query.getTypeName(), query.getFilter(),
                transaction);

    }

    @Override
    protected Name getFeatureTypeName() {
        return entry.getName();
    }

    @Override
    protected SimpleFeature getNextInternal() throws IllegalArgumentException,
            NoSuchElementException, IOException {
        return fw.next();
    }

    @Override
    public SimpleFeature next() throws IOException, IllegalArgumentException,
            NoSuchElementException {
        final SimpleFeature df = super.next();
        fw.write();
        return df;
    }

    @Override
    public boolean hasNext() throws IOException {
        return fw.hasNext();
    }

    @Override
    public void close() throws IOException {
        if (fw != null) {
            try {
                fw.close();
            } catch (IOException e) {
            }
        }
    }

}
