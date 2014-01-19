package org.geotools.data.cache.impl;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.cache.op.BaseFeatureSourceOp;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.cache.utils.DelegateContentFeatureSource;
import org.geotools.data.cache.utils.SimpleFeatureListReader;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureIterator;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.strtree.STRtree;

public class STRFeatureSourceOp extends
		BaseFeatureSourceOp<SimpleFeatureSource> {

	// the cache container
	private STRtree index = new STRtree();

	// lock on cache
	private ReadWriteLock lockIndex = new ReentrantReadWriteLock();

	public STRFeatureSourceOp(CacheManager cacheManager, final String uid)
			throws IOException {
		super(cacheManager, uid);
	}

	@Override
	public SimpleFeatureSource updateCache(Query query) throws IOException {
		updateIndex(querySource(query), queryCachedAreas(query));

		return getCache(query);
	}

	@Override
	public SimpleFeatureSource getCache(Query query) throws IOException {
		verify(query);

		final SimpleFeatureSource featureSource = new DelegateContentFeatureSource(
				cacheManager, getEntry(), query) {
			@Override
			protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(
					Query query) throws IOException {

				return new SimpleFeatureListReader(cacheManager, getSchema(),
						getListOfFeatures(query));
			}
		};
		return featureSource;

	}

	private List<SimpleFeature> getListOfFeatures(Query query) {
		try {
			lockIndex.readLock().lock();
			return index.query(getEnvelope(query.getFilter()));
		} finally {
			lockIndex.readLock().unlock();
		}
	}

	private void updateIndex(Query sourceQuery, Query cachedQuery)
			throws IOException {
		// try to get the schemaOp to use its enrich method (for feature)

		SimpleFeatureSource features = new DelegateContentFeatureSource(
				cacheManager, getEntry(), sourceQuery) {
			@Override
			protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(
					Query query) throws IOException {

				return cacheManager.getSource().getFeatureReader(query,
						org.geotools.data.Transaction.AUTO_COMMIT);
			}
		};

		final FeatureIterator<SimpleFeature> fi = features.getFeatures(
				sourceQuery).features();
		if (fi.hasNext()) {
			final STRtree newIndex = new STRtree();
			do {
				// consider turning all geometries into packed ones, to save
				// space
				final Feature f = fi.next();

				newIndex.insert(ReferencedEnvelope.reference(f.getBounds()), f);

			} while (fi.hasNext());
			// fill with old values
			try {
				lockIndex.readLock().lock();
				walkSTRtree(newIndex, index.itemsTree(),
						getEnvelope(sourceQuery.getFilter()));
			} finally {
				lockIndex.readLock().unlock();
			}
			try {
				lockIndex.writeLock().lock();
				index = newIndex;
			} finally {
				if (fi != null) {
					fi.close();
				}
				lockIndex.writeLock().unlock();
			}
		}
	}

	private static void walkSTRtree(STRtree dst, Object o, Envelope envelope) {
		if (o != null) {
			if (o instanceof Feature) {
				Feature f = (Feature) o;
				ReferencedEnvelope env = ReferencedEnvelope.reference(f
						.getBounds());
				if (!env.intersects(envelope)) {
					dst.insert(env, f);
				}
			} else if (o instanceof List) {
				for (Object oo : ((List) o)) {
					walkSTRtree(dst, oo, envelope);
				}
			}
		}
	}

}
