/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 * 
 *    (C) 2013, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package org.geotools.gce.imagemosaic;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import javax.imageio.spi.ImageReaderSpi;

import org.apache.commons.io.DirectoryWalker;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.FalseFileFilter;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.HiddenFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.data.DefaultTransaction;
import org.geotools.factory.Hints;
import org.geotools.gce.image.WorldImageFormat;
import org.geotools.gce.imagemosaic.Utils.Prop;
import org.geotools.gce.imagemosaic.catalog.GranuleCatalog;
import org.geotools.gce.imagemosaic.catalog.index.Indexer;
import org.geotools.gce.imagemosaic.catalog.index.IndexerUtils;
import org.geotools.gce.imagemosaic.catalog.index.ParametersType;
import org.geotools.gce.imagemosaic.catalog.index.ParametersType.Parameter;
import org.geotools.gce.imagemosaic.catalogbuilder.CatalogBuilderConfiguration;
import org.geotools.geometry.jts.ReferencedEnvelope;

/**
 * This class is in responsible for creating the index for a mosaic of images that we want to tie together as a single coverage.
 * 
 * @author Simone Giannecchini, GeoSolutions
 * @author Carlo Cancellieri - GeoSolutions SAS
 * 
 * @source $URL$
 */
@SuppressWarnings("rawtypes")
public class ImageMosaicDirectoryWalker extends ImageMosaicWalker {

    
    /**
     * This class is responsible for walking through the files inside a directory (and its children directories) which respect a specified wildcard.
     * 
     * <p>
     * Its role is basically to simplify the construction of the mosaic by implementing a visitor pattern for the files that we have to use for the
     * index.
     * 
     * <p>
     * It is based on the Commons IO {@link DirectoryWalker} class.
     * 
     * @author Simone Giannecchini, GeoSolutions SAS
     * @author Daniele Romagnoli, GeoSolutions SAS
     * 
     */
    final class MosaicDirectoryWalker extends DirectoryWalker {

        private ImageMosaicWalker walker;

        @Override
        protected void handleCancelled(File startDirectory, Collection results,
                CancelException cancel) throws IOException {
            super.handleCancelled(startDirectory, results, cancel);
            // clean up objects and rollback transaction
            if (LOGGER.isLoggable(Level.INFO))
                LOGGER.info("Stop requested when walking directory " + startDirectory);
            super.handleEnd(results);
        }

        @Override
        protected boolean handleIsCancelled(final File file, final int depth, Collection results)
                throws IOException {

            //
            // Anyone has asked us to stop?
            //
            if (!checkStop()) {
                canceled = true;
                return true;
            }
            return false;
        }

        @Override
        protected void handleFile(final File fileBeingProcessed, final int depth,
                final Collection results) throws IOException {

            walker.handleFile(fileBeingProcessed);

            super.handleFile(fileBeingProcessed, depth, results);
        }

        private boolean checkStop() {
            if (getStop()) {
                eventHandler.fireEvent(Level.INFO, "Stopping requested at file  " + fileIndex + " of "
                        + numFiles + " files", ((fileIndex * 100.0) / numFiles));
                return false;
            }
            return true;
        }

        private boolean checkFile(final File fileBeingProcessed) {
            if (!fileBeingProcessed.exists() || !fileBeingProcessed.canRead()
                    || !fileBeingProcessed.isFile()) {
                // send a message
                eventHandler.fireEvent(Level.INFO, "Skipped file " + fileBeingProcessed
                        + " snce it seems invalid", ((fileIndex * 99.0) / numFiles));
                return false;
            }
            return true;
        }

        public MosaicDirectoryWalker(final List<String> indexingDirectories,
                final FileFilter filter, ImageMosaicWalker walker) throws IOException {
            super(filter, Integer.MAX_VALUE);// runConfiguration.isRecursive()?Integer.MAX_VALUE:0);

            this.walker = walker;
            transaction = new DefaultTransaction("MosaicCreationTransaction"
                    + System.nanoTime());
            configHandler.indexingPreamble();

            try {
                // start walking directories
                for (String indexingDirectory : indexingDirectories) {
                    walk(new File(indexingDirectory), null);

                    // did we cancel?
                    if (canceled)
                        break;
                }
                // did we cancel?
                if (canceled)
                    transaction.rollback();
                else
                    transaction.commit();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failure occurred while collecting the granules", e);
                transaction.rollback();
            } finally {
                try {
                    configHandler.indexingPostamble(!canceled);
                } catch (Exception e) {
                    final String message = "Unable to close indexing" + e.getLocalizedMessage();
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.log(Level.WARNING, message, e);
                    }
                    // notify listeners
                    eventHandler.fireException(e);
                }

                try {
                    transaction.close();
                } catch (Exception e) {
                    final String message = "Unable to close indexing" + e.getLocalizedMessage();
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.log(Level.WARNING, message, e);
                    }
                    // notify listeners
                    eventHandler.fireException(e);
                }
            }
        }

    }


    private IOFileFilter fileFilter;

    /**
     * run the directory walker
     */
    public void run() {

        try {

            //
            // creating the file filters for scanning for files to check and index
            //
            final IOFileFilter finalFilter = createGranuleFilterRules();

            // TODO we might want to remove this in the future for performance
            numFiles = 0;
            String harvestDirectory = configHandler.getRunConfiguration().getParameter(Prop.HARVEST_DIRECTORY);
            String indexDirs = configHandler.getRunConfiguration().getParameter(Prop.INDEXING_DIRECTORIES);
            if (harvestDirectory != null) {
                indexDirs = harvestDirectory;
            }
            String[] indexDirectories = indexDirs.split("\\s*,\\s*");
            for (String indexingDirectory : indexDirectories) {
                indexingDirectory = Utils.checkDirectory(indexingDirectory, false);
                final File directoryToScan = new File(indexingDirectory);
                final Collection files = FileUtils
                        .listFiles(
                                directoryToScan,
                                finalFilter,
                                Boolean.parseBoolean(configHandler.getRunConfiguration().getParameter(Prop.RECURSIVE)) ? TrueFileFilter.INSTANCE
                                        : FalseFileFilter.INSTANCE);
                numFiles += files.size();
            }
            //
            // walk over the files that have filtered out
            //
            if (numFiles > 0) {
                final List<String> indexingDirectories = new ArrayList<String>(
                        Arrays.asList(indexDirectories));
                @SuppressWarnings("unused")
                final MosaicDirectoryWalker walker = new MosaicDirectoryWalker(indexingDirectories,
                        finalFilter, this);

            }

        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, e.getLocalizedMessage(), e);
        }

    }

    /**
     * @return
     */
    private IOFileFilter createGranuleFilterRules() {
        final IOFileFilter specialWildCardFileFilter = new WildcardFileFilter(
                configHandler.getRunConfiguration().getParameter(Prop.WILDCARD), IOCase.INSENSITIVE);
        IOFileFilter dirFilter = FileFilterUtils.and(FileFilterUtils.directoryFileFilter(),
                HiddenFileFilter.VISIBLE);
        IOFileFilter filesFilter = Utils.excludeFilters(FileFilterUtils
                .makeSVNAware(FileFilterUtils.makeFileOnly(FileFilterUtils.and(
                        specialWildCardFileFilter, HiddenFileFilter.VISIBLE))), FileFilterUtils
                .suffixFileFilter("shp"), FileFilterUtils.suffixFileFilter("dbf"), FileFilterUtils
                .suffixFileFilter("shx"), FileFilterUtils.suffixFileFilter("qix"), FileFilterUtils
                .suffixFileFilter("lyr"), FileFilterUtils.suffixFileFilter("prj"), FileFilterUtils
                .nameFileFilter("error.txt"), FileFilterUtils.nameFileFilter("error.txt.lck"),
                FileFilterUtils.suffixFileFilter("properties"), FileFilterUtils
                        .suffixFileFilter("svn-base"));

        // exclude common extensions
        Set<String> extensions = WorldImageFormat.getWorldExtension("png");
        for (String ext : extensions) {
            filesFilter = FileFilterUtils.and(filesFilter, FileFilterUtils
                    .notFileFilter(FileFilterUtils.suffixFileFilter(ext.substring(1))));
        }
        extensions = WorldImageFormat.getWorldExtension("gif");
        for (String ext : extensions) {
            filesFilter = FileFilterUtils.and(filesFilter, FileFilterUtils
                    .notFileFilter(FileFilterUtils.suffixFileFilter(ext.substring(1))));
        }
        extensions = WorldImageFormat.getWorldExtension("jpg");
        for (String ext : extensions) {
            filesFilter = FileFilterUtils.and(filesFilter, FileFilterUtils
                    .notFileFilter(FileFilterUtils.suffixFileFilter(ext.substring(1))));
        }
        extensions = WorldImageFormat.getWorldExtension("tiff");
        for (String ext : extensions) {
            filesFilter = FileFilterUtils.and(filesFilter, FileFilterUtils
                    .notFileFilter(FileFilterUtils.suffixFileFilter(ext.substring(1))));
        }
        extensions = WorldImageFormat.getWorldExtension("bmp");
        for (String ext : extensions) {
            filesFilter = FileFilterUtils.and(filesFilter, FileFilterUtils
                    .notFileFilter(FileFilterUtils.suffixFileFilter(ext.substring(1))));
        }

        // sdw
        filesFilter = FileFilterUtils.and(filesFilter,
                FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter("sdw")) // );
                // aux
                // fileFilter = FileFilterUtils.andFileFilter(fileFilter,
                , FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter("aux"))// );
                // wld
                // fileFilter = FileFilterUtils.andFileFilter(fileFilter,
                , FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter("wld"))// );
                // svn
                // fileFilter = FileFilterUtils.andFileFilter(fileFilter,
                , FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter("svn")));

        if (this.fileFilter != null) {
            filesFilter = FileFilterUtils.and(this.fileFilter, filesFilter);
        }

        final IOFileFilter finalFilter = FileFilterUtils.or(dirFilter, filesFilter);
        return finalFilter;
    }

    /**
     * Default constructor
     * 
     * @throws
     * @throws IllegalArgumentException
     */
    public ImageMosaicDirectoryWalker(ImageMosaicConfigHandler configHandler,
            ImageMosaicEventHandlers eventHandler) {
        super(configHandler, eventHandler);
    }
//
//    private void updateConfigurationHints(final CatalogBuilderConfiguration configuration,
//            Hints hints, final String ancillaryFile, final String rootMosaicDir) {
//        if (ancillaryFile != null) {
//            final String ancillaryFilePath = rootMosaicDir + File.separatorChar + ancillaryFile;
//            if (hints != null) {
//                hints.put(Utils.AUXILIARY_FILES_PATH, ancillaryFilePath);
//            } else {
//                hints = new Hints(Utils.AUXILIARY_FILES_PATH, ancillaryFilePath);
//                configuration.setHints(hints);
//            }
//        }
//
//        if (hints != null && hints.containsKey(Utils.MOSAIC_READER)) {
//            Object reader = hints.get(Utils.MOSAIC_READER);
//            if (reader instanceof ImageMosaicReader) {
//                configHandler.setParentReader((ImageMosaicReader) reader);
//                Hints readerHints = configHandler.getParentReader().getHints();
//                readerHints.add(hints);
//            }
//        }
//    }

    /**
     * Setup default params to the indexer.
     * 
     * @param params
     * @param indexer
     */
    private void copyDefaultParams(ParametersType params, Indexer indexer) {
        if (params != null) {
            List<Parameter> defaultParamList = params.getParameter();
            if (defaultParamList != null && !defaultParamList.isEmpty()) {
                ParametersType parameters = indexer.getParameters();
                if (parameters == null) {
                    parameters = Utils.OBJECT_FACTORY.createParametersType();
                    indexer.setParameters(parameters);
                }
                List<Parameter> parameterList = parameters.getParameter();
                for (Parameter defaultParameter : defaultParamList) {
                    final String defaultParameterName = defaultParameter.getName();
                    if (IndexerUtils.getParameter(defaultParameterName, indexer) == null) {
                        IndexerUtils.setParam(parameterList, defaultParameterName,
                                defaultParameter.getValue());
                    }
                }
            }
        }
    }


//
//
//    private void closeIndexObjects() {
//
//        // TODO: We may consider avoid disposing the catalog to allow the reader to use the already available catalog
//        try {
//            if (catalog != null) {
//                catalog.dispose();
//            }
//        } catch (Throwable e) {
//            LOGGER.log(Level.SEVERE, e.getLocalizedMessage(), e);
//        }
//
//        catalog = null;
//        parentReader.granuleCatalog = null;
//    }


    public IOFileFilter getFileFilter() {
        return fileFilter;
    }

    /**
     * Sets a filter that can reduce the file the mosaic walker will take into consideration (in a more flexible way than the wildcards)
     * 
     * @param fileFilter
     */
    public void setFileFilter(IOFileFilter fileFilter) {
        this.fileFilter = fileFilter;
    }

}
