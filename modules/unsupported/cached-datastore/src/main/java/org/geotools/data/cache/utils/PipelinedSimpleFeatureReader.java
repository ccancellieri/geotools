package org.geotools.data.cache.utils;

import java.io.IOException;

import org.geotools.data.cache.op.CacheManager;
import org.geotools.data.simple.SimpleFeatureReader;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * 
 * @author carlo cancellieri - GeoSolutions SAS
 * 
 */
public class PipelinedSimpleFeatureReader extends DelegateSimpleFeatureReader {

	private final SimpleFeatureReader[] frc;

	private int currentReader = 0;

	public PipelinedSimpleFeatureReader(CacheManager cacheManager,
			SimpleFeatureType schema, SimpleFeatureReader... coll)
			throws IOException {

		super(cacheManager, schema);

		if (coll == null || coll.length == 0) {
			throw new IllegalArgumentException("Unable to create a "
					+ this.getClass()
					+ " with a null or empty list fo FeatureCollection");
		}

		this.frc = coll;
		setDelegate(coll[0]);
	}

	private SimpleFeatureReader nextReader() throws IOException {
		SimpleFeatureReader delegate = frc[this.currentReader++];
		if (schema != null && schema.equals(delegate.getFeatureType())) {
			throw new IOException(
					"Unable to read from collections with different schemas");
		}
		return delegate;
	}

	private boolean hasNextCollection() {
		return this.currentReader < frc.length;
	}

	@Override
	public boolean hasNext() throws IOException {
		if (delegate.hasNext()) {
			return true;
		} else if (hasNextCollection()) {
			delegate.close();
			delegate = nextReader();
			return hasNext();
		} else {
			return false;
		}
	}

	@Override
	public void close() throws IOException {
		do {
			try {
				delegate.close();
			} catch (Exception e) {
			}
			delegate = nextReader();
		} while (hasNextCollection());
	}

}
