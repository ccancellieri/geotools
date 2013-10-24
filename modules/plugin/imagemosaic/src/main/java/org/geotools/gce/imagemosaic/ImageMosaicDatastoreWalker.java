package org.geotools.gce.imagemosaic;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.DirectoryWalker;
import org.apache.commons.io.FilenameUtils;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.GridFormatFinder;
import org.geotools.coverage.grid.io.UnknownFormat;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.factory.Hints;
import org.geotools.util.Utilities;
import org.opengis.feature.simple.SimpleFeature;

/**
 * This class is responsible for walking through the target schema and check all
 * the located granules.
 * 
 * <p>
 * Its role is basically to simplify the construction of the mosaic by
 * implementing a visitor pattern for the files that we have to use for the
 * index.
 * 
 * 
 * @author Carlo Cancellieri - GeoSolutions SAS
 * 
 * @TODO check the schema structure
 * 
 */
class ImageMosaicDatastoreWalker extends ImageMosaicWalker {

	/** Default Logger * */
	final static Logger LOGGER = org.geotools.util.logging.Logging
			.getLogger(ImageMosaicDatastoreWalker.class);

	protected DefaultTransaction transaction;

	private volatile boolean canceled = false;

	/**
	 * @param updateFeatures
	 *            if true update catalog with loaded granules
	 * @param imageMosaicConfigHandler
	 *            TODO
	 */
	public ImageMosaicDatastoreWalker(ImageMosaicConfigHandler configHandler,
			ImageMosaicEventHandlers eventHandler) {
		super(configHandler, eventHandler);
	}

	/**
	 * run the directory walker
	 */
	public void run() {

		try {

			this.transaction = new DefaultTransaction(
					"MosaicCreationTransaction" + System.nanoTime());

			configHandler.indexingPreamble();

			try {
				// start looking into catalog
				for (String typeName : configHandler.getCatalog()
						.getTypeNames()) {

					final Query query = new Query(typeName);
					final SimpleFeatureCollection coll = configHandler
							.getCatalog().getGranules(query);
					// TODO we might want to remove this in the future for
					// performance
					numFiles = coll.size();

					SimpleFeatureIterator it = null;
					try {
						it = coll.features();
						// TODO setup index name

						// final MosaicConfigurationBean config=
						// configurations.get(typeName);
						while (it.hasNext()) {
							SimpleFeature feature = it.next();
							// String
							// locationAttrName=config.getCatalogConfigurationBean().getLocationAttribute();
							String locationAttrName = configHandler
									.getRunConfiguration()
									.getLocationAttribute();
							Object locationAttrObj = feature
									.getAttribute(locationAttrName);
							File indexing = null;
							if (locationAttrObj instanceof String) {
								if (configHandler.getRunConfiguration()
										.isAbsolute()) {
									indexing = new File(
											(String) locationAttrObj);
								} else {
									indexing = new File(configHandler
											.getRunConfiguration()
											.getRootMosaicDirectory(),
											(String) locationAttrObj);
								}

							} else if (locationAttrObj instanceof File) {
								indexing = (File) locationAttrObj;
							} else {
								eventHandler.fireException(new IOException(
										"Location attribute type not recognized for column name: "
												+ locationAttrName));
								canceled = true;
								break;
							}
							handleFile(indexing);
						}
					} finally {
						if (it != null)
							it.close();
					}

					// did we cancel?
					if (canceled)
						transaction.rollback();
					else
						transaction.commit();
				}

			} catch (Exception e) {
				LOGGER.log(Level.WARNING,
						"Failure occurred while collecting the granules", e);
				transaction.rollback();
			} finally {
				try {
					configHandler.indexingPostamble(!canceled);
				} catch (Exception e) {
					final String message = "Unable to close indexing"
							+ e.getLocalizedMessage();
					if (LOGGER.isLoggable(Level.WARNING)) {
						LOGGER.log(Level.WARNING, message, e);
					}
					// notify listeners
					eventHandler.fireException(e);
				}

				try {
					transaction.close();
				} catch (Exception e) {
					final String message = "Unable to close indexing"
							+ e.getLocalizedMessage();
					if (LOGGER.isLoggable(Level.WARNING)) {
						LOGGER.log(Level.WARNING, message, e);
					}
					// notify listeners
					eventHandler.fireException(e);
				}
			}

			// }

		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, e.getLocalizedMessage(), e);
		}

	}
}