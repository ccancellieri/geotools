package org.geotools.data.cache.utils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.geotools.data.cache.op.CacheManager;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class SimpleFeatureListReader extends DelegateSimpleFeatureReader {

	private final Iterator<SimpleFeature> it;

	public SimpleFeatureListReader(CacheManager cacheManager,
			SimpleFeatureType schema, List<SimpleFeature> coll)
			throws IOException {
		super(cacheManager, schema);
		it = coll.iterator();
	}

	@Override
	public SimpleFeature getNextInternal() throws IOException,
			IllegalArgumentException, NoSuchElementException {
		return it.next();
	}

	@Override
	public boolean hasNext() throws IOException {
		return it.hasNext();
	}

	@Override
	public void close() throws IOException {
		// do nothing
	}

}
