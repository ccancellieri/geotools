package org.geotools.data.cache.utils;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.cache.datastore.CacheManager;
import org.geotools.data.store.ContentEntry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

public class SimpleFeatureReader extends DelegateSimpleFeature {

    private final ContentEntry entry;

    // private final Transaction transaction;

    private FeatureReader<SimpleFeatureType, SimpleFeature> fr = null;

    public SimpleFeatureReader(ContentEntry entry, final Query query,
            final CacheManager cacheManager) throws IOException {
        this(entry, query, cacheManager, Transaction.AUTO_COMMIT);
    }

    public SimpleFeatureReader(ContentEntry entry, final Query query,
            final CacheManager cacheManager, final Transaction transaction) throws IOException {

        super(cacheManager);

        this.entry = entry;

        // this.transaction = transaction;

        fr = cacheManager.getCache().getFeatureReader(query, transaction);
    }

    @Override
    protected Name getFeatureTypeName() {
        return entry.getName();
    }

    @Override
    protected SimpleFeature getNextInternal() throws IllegalArgumentException,
            NoSuchElementException, IOException {
        return fr.next();
    }

    @Override
    public SimpleFeature next() throws IOException, IllegalArgumentException,
            NoSuchElementException {
        return super.next();
    }

    @Override
    public boolean hasNext() throws IOException {
        return fr.hasNext();
    }

    @Override
    public void close() throws IOException {
        if (fr != null) {
            try {
                fr.close();
            } catch (IOException e) {
            }
        }
    }

}
