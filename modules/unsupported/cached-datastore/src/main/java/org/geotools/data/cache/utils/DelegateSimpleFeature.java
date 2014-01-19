package org.geotools.data.cache.utils;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.cache.op.NextOp;
import org.geotools.data.cache.op.Operation;
import org.geotools.data.cache.op.SchemaOp;
import org.geotools.data.simple.SimpleFeatureReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

/**
 * 
 * @author carlo cancellieri - GeoSolutions SAS
 * 
 */
public abstract class DelegateSimpleFeature implements SimpleFeatureReader {

	protected final transient Logger LOGGER = org.geotools.util.logging.Logging
			.getLogger(getClass().getPackage().getName());

	protected final SchemaOp schemaOp;
	protected final NextOp nextOp;
	protected final CacheManager cacheManager;

	public DelegateSimpleFeature(CacheManager cacheManager) throws IOException {

		if (cacheManager == null) {
			throw new IllegalArgumentException("Unable to create a "
					+ this.getClass() + " with a null cacheManager");
		}
		this.cacheManager = cacheManager;

		this.schemaOp = cacheManager.getCachedOpOfType(Operation.schema,
				SchemaOp.class);
		this.nextOp = cacheManager.getCachedOpOfType(Operation.next,
				NextOp.class);
	}

	protected abstract Name getFeatureTypeName();

	@Override
	public SimpleFeatureType getFeatureType() {
		SimpleFeatureType cachedSchema = null;
		if (schemaOp != null) {
			try {
				if (!schemaOp.isCached(getFeatureTypeName())
						|| schemaOp.isDirty(getFeatureTypeName())) {
					cachedSchema = schemaOp.updateCache(getFeatureTypeName());
					schemaOp.setCached(getFeatureTypeName(), cachedSchema != null ? true
							: false);
				} else {
					cachedSchema = schemaOp.getCache(getFeatureTypeName());
				}
			} catch (IOException e) {
				if (LOGGER.isLoggable(Level.SEVERE)) {
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
				}
			}
		}
		if (cachedSchema != null) {
			return cachedSchema;
		} else {
			try {
				return cacheManager.getSource().getSchema(getFeatureTypeName());
			} catch (IOException e) {
				if (LOGGER.isLoggable(Level.SEVERE)) {
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
				}
				return null;
			}
		}
	}

	protected abstract SimpleFeature getNextInternal()
			throws IllegalArgumentException, NoSuchElementException,
			IOException;

	@Override
	public SimpleFeature next() throws IOException, IllegalArgumentException,
			NoSuchElementException {

		final SimpleFeature sf = getNextInternal();
		// try using next operation
		if (nextOp != null) {
			SimpleFeature feature = null;
			if (!nextOp.isCached(sf)
					|| nextOp.isDirty(sf)) {
				feature = nextOp.updateCache(sf);
				nextOp.setCached(sf, feature != null ? true
						: false);
			} else {
				feature = nextOp.getCache(sf);
			}
			if (feature != null) {
				return feature;
			}
		}
		return sf;
	}

	@Override
	public abstract boolean hasNext() throws IOException;

	@Override
	public abstract void close() throws IOException;

}
