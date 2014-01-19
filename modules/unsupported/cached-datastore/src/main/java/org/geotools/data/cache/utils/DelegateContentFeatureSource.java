package org.geotools.data.cache.utils;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.cache.op.BaseFeatureSourceOp;
import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.cache.op.CachedOp;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.cache.op.SchemaOp;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

/**
 * Use CacheManager to try cached operations if no operation is defined then
 * delegates to the CachedOps found into the passed {@link CacheManager}
 * 
 * @author carlo cancellieri - GeoSolutions SAS
 * 
 */
@SuppressWarnings("unchecked")
public class DelegateContentFeatureSource extends ContentFeatureSource {

	private final transient Logger LOGGER = org.geotools.util.logging.Logging
			.getLogger(getClass().getPackage().getName());

	// source
	private final SimpleFeatureSource delegate;

	protected final CacheManager cacheManager;

	final SchemaOp schemaOp;

	public SimpleFeatureSource getDelegate() {
		return delegate;
	}

	public DelegateContentFeatureSource(final CacheManager cacheManager,
			final ContentEntry entry, final Query query)
			throws IllegalArgumentException, IOException {
		super(entry, query);
		if (cacheManager == null)
			throw new IllegalArgumentException(
					"Unable to initialize a cache manager !=null is needed");

		this.cacheManager = cacheManager;

		this.delegate = cacheManager.getSource().getFeatureSource(
				entry.getTypeName());

		this.schemaOp = cacheManager.getCachedOpOfType(Operation.schema,
				SchemaOp.class);
	}

	@Override
	protected ReferencedEnvelope getBoundsInternal(Query query)
			throws IOException {
		final CachedOp<ReferencedEnvelope, Query> op = cacheManager
				.getCachedOpOfType(Operation.bounds, CachedOp.class);
		ReferencedEnvelope env = null;
		if (op != null) {
			try {
				if (!op.isCached(query) || op.isDirty(query)) {
					env = op.updateCache(query);
					op.setCached(query, env != null ? true : false);
				} else {
					env = op.getCache(query);
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
			return delegate.getBounds(query);
		}

	}

	@Override
	protected int getCountInternal(Query query) throws IOException {
		final CachedOp<Integer, Query> op = cacheManager.getCachedOpOfType(
				Operation.count, CachedOp.class);
		Integer count = null;
		if (op != null) {
			try {
				if (!op.isCached(query) || op.isDirty(query)) {
					count = op.updateCache(query);
					op.setCached(query, count != null ? true : false);
				} else {
					count = op.getCache(query);
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
			return delegate.getCount(query);
		}
	}

	@Override
	protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(
			Query query) throws IOException {
		final BaseFeatureSourceOp<SimpleFeatureSource> op = cacheManager
				.getCachedOpOfType(Operation.featureSource,
						BaseFeatureSourceOp.class);
		if (op != null) {
			op.setEntry(getEntry());
			op.setSchema(getSchema());
			try {
				if (!op.isCached(query) || op.isDirty(query)) {

					final SimpleFeatureSource source = op.updateCache(query);
					if (source != null) {
						op.setCached(query, true);
						return new SimpleFeatureCollectionReader(cacheManager,
								getSchema(), source.getFeatures(query));
					} else {
						op.setCached(query, false);
						throw new IOException(
								"Unable to create a simple feature source from the passed query: "
										+ query);
					}
				}
				return new SimpleFeatureCollectionReader(cacheManager,
						getSchema(), op.getCache(query).getFeatures());
			} catch (IOException e) {
				if (LOGGER.isLoggable(Level.SEVERE)) {
					LOGGER.log(Level.SEVERE, e.getLocalizedMessage(), e);
				}
				throw e;
			}
		}
		if (LOGGER.isLoggable(Level.WARNING)) {
			LOGGER.log(Level.WARNING, "No cached operation is found: "
					+ Operation.featureSource);
		}
		// if (source != null) {
		// return new SimpleFeatureCollectionReader(cacheManager,
		// source.getFeatures()); // TODO pass the query????
		// } else {
		return new SimpleFeatureCollectionReader(cacheManager, getSchema(),
				delegate.getFeatures(query));
		// }
	}

	@Override
	protected SimpleFeatureType buildFeatureType() throws IOException {
		SimpleFeatureType schema = null;
		if (schemaOp != null) {
			final Name name = getEntry().getName();
			try {
				if (!schemaOp.isCached(name) || schemaOp.isDirty(name)) {
					schema = schemaOp.updateCache(name);
					schemaOp.setCached(name, schema != null ? true : false);
				} else {
					schema = schemaOp.getCache(name);
				}
			} catch (IOException e) {
				if (LOGGER.isLoggable(Level.SEVERE)) {
					LOGGER.log(Level.SEVERE, e.getLocalizedMessage(), e);
				}
			}
		}
		if (schema != null) {
			return schema;
		} else {
			return delegate.getSchema();
		}
	}

}
