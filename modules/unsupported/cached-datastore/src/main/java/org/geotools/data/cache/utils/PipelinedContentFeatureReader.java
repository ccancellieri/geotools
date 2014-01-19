package org.geotools.data.cache.utils;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.store.ContentEntry;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

public class PipelinedContentFeatureReader extends DelegateSimpleFeature
		implements FeatureReader<SimpleFeatureType, SimpleFeature> {

	private final ContentEntry entry;

	private final Transaction transaction;

	private FeatureWriter<SimpleFeatureType, SimpleFeature> fw = null;

	private FeatureReader<SimpleFeatureType, SimpleFeature> fr = null;

	private FeatureReader<SimpleFeatureType, SimpleFeature> frDiff = null;

	public PipelinedContentFeatureReader(ContentEntry entry,
			final Query sourceQuery, final Query cachedAreasQuery,
			final CacheManager cacheManager) throws IOException {
		this(entry, sourceQuery, cachedAreasQuery, cacheManager,
				Transaction.AUTO_COMMIT);
	}

	public PipelinedContentFeatureReader(ContentEntry entry,
			final Query sourceQuery, final Query cachedAreasQuery,
			final CacheManager cacheManager, final Transaction transaction)
			throws IOException {
		super(cacheManager);
		this.entry = entry;

		this.transaction = transaction;

		fr = cacheManager.getSource()
				.getFeatureReader(sourceQuery, transaction);
		fw = cacheManager.getCache().getFeatureWriter(
				sourceQuery.getTypeName(), sourceQuery.getFilter(),
				// //java.util.NoSuchElementException:
				// FeatureWriter does not have
				// additional content
				transaction);

		frDiff = cacheManager.getCache().getFeatureReader(cachedAreasQuery,
				transaction);
	}

	@Override
	protected Name getFeatureTypeName() {
		return entry.getName();
	}

	@Override
	protected SimpleFeature getNextInternal() throws IllegalArgumentException,
			NoSuchElementException, IOException {
		if (!fw.hasNext()) {
			try {
				fw.close();
			} catch (Exception e) {

			}
			fw = cacheManager.getCache().getFeatureWriterAppend(
					getFeatureTypeName().getLocalPart(), transaction);
		}
		final SimpleFeature df = fw.next();
		for (final Property p : fr.next().getProperties()) {
			df.setAttribute(p.getName(), p.getValue());
		}
		return df;
	}

	@Override
	public SimpleFeature next() throws IOException, IllegalArgumentException,
			NoSuchElementException {
		if (fr.hasNext()) {
			final SimpleFeature df = super.next();
			fw.write();
			return df;
		} else {
			// conclude returning the diff
			return frDiff.next();
		}
	}

	@Override
	public boolean hasNext() throws IOException {
		boolean notEnd = fr.hasNext() || frDiff.hasNext();
		// at the end clear remaining (dirty) features
		// if (!notEnd) {
		// // TODO check me
		// while (fw.hasNext()) {
		// fw.next();
		// fw.remove();
		// }
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
		if (frDiff != null) {
			try {
				frDiff.close();
			} catch (IOException e) {
			}
		}
	}

}
