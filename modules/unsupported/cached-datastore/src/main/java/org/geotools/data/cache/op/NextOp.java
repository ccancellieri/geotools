package org.geotools.data.cache.op;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.logging.Logger;

import org.geotools.feature.simple.SimpleSchema;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;

public class NextOp extends BaseOp<SimpleFeature, SimpleFeature> {

	private final transient Logger LOGGER = org.geotools.util.logging.Logging
			.getLogger(getClass().getPackage().getName());

	public NextOp(CacheManager cacheManager, final String uniqueName) {
		super(cacheManager, uniqueName);
	}

	@Override
	public SimpleFeature updateCache(SimpleFeature sf) throws IOException {
		verify(sf);

		// consider turning all geometries into packed ones, to save space
		for (final Property p : enrich(sf, false, LOGGER)) {
			sf.getProperty(p.getName()).setValue(p.getValue());
		}
		//
		// sf.setValue(enrich(sf, false, LOGGER));

		return sf;

	}

	@Override
	public SimpleFeature getCache(SimpleFeature o) throws IOException {
		return updateCache(o);
	}

	private static Collection<Property> enrich(SimpleFeature sourceF,
			boolean updateTimestamp, Logger LOGGER) throws IOException {

		final Collection<Property> props = sourceF.getProperties();

		final Property hints = sourceF.getProperty(SchemaOp.HINTS_NAME);
		if (hints != null) {
			final Class c = hints.getType().getBinding();
			if (SimpleSchema.LONG.getBinding().isAssignableFrom(c)) {
				final Object o = hints.getValue();
				if (o != null) {
					final Long oldValue = (Long) SimpleSchema.LONG.getBinding()
							.cast(o);
					hints.setValue(oldValue + 1);
				} else {
					hints.setValue(0L);
				}
			} else {
				throw new IOException(
						"Unable to enrich this feature: wrong binding class ("
								+ c + ") for property: " + hints.getName());
			}
		}

		final Property timestamp = sourceF.getProperty(SchemaOp.TIMESTAMP_NAME);
		if (timestamp != null) {
			final Class c = timestamp.getType().getBinding();
			if (SimpleSchema.DATETIME.getBinding().isAssignableFrom(c)) {
				final Object o = timestamp.getValue();
				if (updateTimestamp) {
					timestamp.setValue(new Timestamp(Calendar.getInstance()
							.getTimeInMillis()));
				} else if (o != null) {
					final Timestamp oldValue = (Timestamp) SimpleSchema.DATETIME
							.getBinding().cast(o);
					timestamp.setValue(oldValue);
				}
			} else if (Date.class.isAssignableFrom(c)) {
				final Object o = timestamp.getValue();
				if (updateTimestamp) {
					timestamp.setValue(new Timestamp(Calendar.getInstance()
							.getTimeInMillis()));
				} else if (o != null) {
					Date date = (Date) o;
					final Timestamp oldValue = new Timestamp(date.getTime());
					timestamp.setValue(oldValue);
				}
			} else {
				throw new IOException(
						"Unable to enrich this feature: wrong binding class ("
								+ c + ") for property: " + timestamp.getName());
			}
		}
		return props;
	}

	@Override
	public void setCached(SimpleFeature key, boolean isCached) {
		// do nothing
	};

	@Override
	public boolean isCached(SimpleFeature o) throws IOException {
		return true;
	}

	// public SimpleFeature getFeature() {
	// return sf;
	// }
	//
	// public void setFeature(SimpleFeature sf) {
	// this.sf = sf;
	// }

	// public SimpleFeature getDf() {
	// return df;
	// }
	//
	// public void setDf(SimpleFeature df) {
	// this.df = df;
	// }

	@Override
	public boolean isDirty(SimpleFeature key) throws IOException {
		return false;
	}

	@Override
	public void setDirty(SimpleFeature query) throws IOException {
		throw new UnsupportedOperationException();
	}

}
