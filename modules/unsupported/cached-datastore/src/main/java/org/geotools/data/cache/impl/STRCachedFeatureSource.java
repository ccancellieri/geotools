package org.geotools.data.cache.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.geotools.data.DataSourceException;
import org.geotools.data.DefaultQuery;
import org.geotools.data.FeatureReader;
import org.geotools.data.FilteringFeatureReader;
import org.geotools.data.Query;
import org.geotools.data.cache.utils.DelegateContentFeatureSource;
import org.geotools.data.cache.utils.SimpleListReader;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentEntry;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.filter.And;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.spatial.BinarySpatialOperator;
import org.opengis.filter.spatial.Contains;
import org.opengis.filter.spatial.Crosses;
import org.opengis.filter.spatial.DWithin;
import org.opengis.filter.spatial.Equals;
import org.opengis.filter.spatial.Intersects;
import org.opengis.filter.spatial.Overlaps;
import org.opengis.filter.spatial.Touches;
import org.opengis.filter.spatial.Within;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.strtree.STRtree;

@SuppressWarnings("unchecked")
public class STRCachedFeatureSource extends DelegateContentFeatureSource {

    private STRtree index;

    private ReadWriteLock lockIndex = new ReentrantReadWriteLock();

    private boolean dirty = true;

    // private Query cachedQuery;
    //
    // private Envelope cachedBounds;

    private Geometry cachedAreas = new Polygon(null, null, JTSFactoryFinder.getGeometryFactory());

    private ReadWriteLock lockAreas = new ReentrantReadWriteLock();

    private final Envelope originalBounds;

    private final SimpleFeatureType schema;

    static FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2(null);

    private static final Set<Class<? extends Filter>> supportedFilterTypes = new HashSet<Class<? extends Filter>>(
            Arrays.asList(BBOX.class, Contains.class, Crosses.class, DWithin.class, Equals.class,
                    Intersects.class, Overlaps.class, Touches.class, Within.class));

    public STRCachedFeatureSource(SimpleFeatureSource sfs, ContentEntry entry, Query query)
            throws IllegalArgumentException, IOException {
        super(entry, query, sfs);
        if (delegate == null) {
            throw new IllegalArgumentException(
                    "Unable to initialize without a source ContentFeatureStore");
        }
        Envelope bounds = sfs.getBounds();
        if (bounds == null)
            originalBounds = new Envelope(-Double.MAX_VALUE, Double.MAX_VALUE, -Double.MAX_VALUE,
                    Double.MAX_VALUE);
        else
            originalBounds = bounds;

        schema = sfs.getSchema();
    }

    @Override
    public ReferencedEnvelope getBoundsInternal(Query query) {
        return ReferencedEnvelope.reference(originalBounds);
    }

    @Override
    public SimpleFeatureType buildFeatureType() {
        return schema;
    }

    @Override
    public int getCountInternal(Query query) throws IOException {
        return getFeatures(query).size();
    }

    /**
     * integrate the passed index with the passed spatial query (less restrictive than the query itself)
     * 
     * @param query
     * @return
     * @throws IOException
     * @throws FactoryException
     * @throws TransformException
     * @throws MismatchedDimensionException
     */
    void integrateCache(Query query) throws IOException, FactoryException,
            MismatchedDimensionException, TransformException {

        Query cloned = new DefaultQuery(query);
        boolean isEmpty = true;
        try {
            lockAreas.readLock().lock();
            isEmpty = cachedAreas.isEmpty();
        } finally {
            lockAreas.readLock().unlock();
        }
        if (!isEmpty) {
            SimpleFeatureType schema = getSchema();
            GeometryDescriptor geoDesc = schema.getGeometryDescriptor();
            String geoName = geoDesc.getLocalName();
            Filter[] sF = splitFilters(cloned);
            Envelope env = getEnvelope(sF[1]);
            Geometry geom = JTS.toGeometry(env);
            CoordinateReferenceSystem targetCRS = query.getCoordinateSystemReproject();
            CoordinateReferenceSystem worldCRS = geoDesc.getCoordinateReferenceSystem();
            MathTransform transform = CRS.findMathTransform(worldCRS, targetCRS != null ? targetCRS
                    : worldCRS);

            try {
                lockAreas.readLock().lock();
                cloned.setFilter(ff.intersects(ff.property(geoName), ff.literal(JTS.transform(
                        geom.difference(cachedAreas.getEnvelope()), transform))));
            } finally {
                lockAreas.readLock().unlock();
            }
        }

        // cachedQuery = cloned;
        updateCache(cloned);
        try {
            lockAreas.writeLock().lock();
            cachedAreas = cachedAreas.union(JTS.toGeometry(getEnvelope(cloned.getFilter())));
        } finally {
            lockAreas.writeLock().unlock();
        }
        // cachedBounds = cachedAreas.getEnvelopeInternal();
        dirty = false;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    private void updateCache(Query query) throws IOException {
        FeatureCollection<SimpleFeatureType, SimpleFeature> features = delegate.getFeatures(query);
        FeatureIterator<SimpleFeature> fi = null;
        try {
            lockIndex.writeLock().lock();
            STRtree newIndex = new STRtree();
            fi = features.features();
            while (fi.hasNext()) {
                // consider turning all geometries into packed ones, to save space
                Feature f = fi.next();
                newIndex.insert(ReferencedEnvelope.reference(f.getBounds()), f);
            }
            if (!dirty) {
                // fill with old values
                walkSTRtree(newIndex, index.itemsTree());
            }
            index = newIndex;
        } finally {
            if (fi != null) {
                fi.close();
            }
            lockIndex.writeLock().unlock();
        }
    }

    private static void walkSTRtree(STRtree dst, Object o) {
        if (o != null) {
            if (o instanceof Feature) {
                Feature f = (Feature) o;
                dst.insert(ReferencedEnvelope.reference(f.getBounds()), f);
            } else if (o instanceof List) {
                for (Object oo : ((List) o)) {
                    walkSTRtree(dst, oo);
                }
            }
        }
    }

    boolean isSubArea(Query query) {
        try {
            lockAreas.readLock().lock();
            // no cached data?
            if (cachedAreas == null)
                return false;
            Filter[] filters = splitFilters(query);
            Envelope envelope = getEnvelope(filters[1]);
            return cachedAreas.contains(JTS.toGeometry(envelope));
        } finally {
            lockAreas.readLock().unlock();
        }
    }

    Envelope getEnvelope(Filter filter) {
        Envelope result = new Envelope();
        if (filter instanceof And) {
            Envelope bounds = new Envelope();
            for (Iterator iter = ((And) filter).getChildren().iterator(); iter.hasNext();) {
                Filter f = (Filter) iter.next();
                Envelope e = getEnvelope(f);
                if (e == null)
                    return null;
                else
                    bounds.expandToInclude(e);
            }
            result = bounds;
        } else if (filter instanceof BinarySpatialOperator) {
            BinarySpatialOperator gf = (BinarySpatialOperator) filter;

            for (Class c : gf.getClass().getInterfaces()) {
                if (supportedFilterTypes.contains(c)) {
                    Expression le = gf.getExpression1();
                    Expression re = gf.getExpression2();
                    if (le instanceof PropertyName && re instanceof Literal) {
                        // String lp = ((PropertyName) le).getPropertyName();
                        Object rl = ((Literal) re).getValue();
                        if (rl instanceof Geometry) {
                            Geometry g = (Geometry) rl;
                            result = g.getEnvelopeInternal();
                        }
                    } else if (le instanceof Literal && re instanceof Literal) {
                        Object ll = ((Literal) le).getValue();
                        Object rl = ((Literal) re).getValue();
                        if (ll instanceof Geometry && rl instanceof PropertyName) {
                            Geometry g = (Geometry) ll;
                            result = g.getEnvelopeInternal();
                        } else if (ll instanceof PropertyName && rl instanceof Geometry) {
                            Geometry g = (Geometry) rl;
                            result = g.getEnvelopeInternal();
                        }
                    }
                    break;
                }
            }
        }
        return result.intersection(originalBounds);
    }

    /**
     * Splits a query into two parts, a spatial component that can be turned into a bbox filter (by including some more feature in the result) and a
     * residual component that we cannot address with the spatial index
     * 
     * @param query
     */
    Filter[] splitFilters(Query query) {
        Filter filter = query.getFilter();
        if (filter == null || filter.equals(Filter.EXCLUDE)) {
            return new Filter[] { Filter.EXCLUDE, bboxFilter(originalBounds) };
        }

        if (!(filter instanceof And)) {
            Envelope envelope = getEnvelope(filter);
            if (envelope == null)
                return new Filter[] { Filter.EXCLUDE, bboxFilter(originalBounds) };
            else
                return new Filter[] { Filter.EXCLUDE, bboxFilter(envelope) };
        }

        And and = (And) filter;
        List residuals = new ArrayList();
        List bboxBacked = new ArrayList();
        for (Iterator it = and.getChildren().iterator(); it.hasNext();) {
            Filter child = (Filter) it.next();
            if (getEnvelope(child) != null) {
                bboxBacked.add(child);
            } else {
                residuals.add(child);
            }
        }

        return new Filter[] { (Filter) ff.and(residuals), (Filter) ff.and(bboxBacked) };
    }

    private BBOX bboxFilter(Envelope bbox) {
        return ff.bbox(getSchema().getGeometryDescriptor().getLocalName(), bbox.getMinX(),
                bbox.getMinY(), bbox.getMaxX(), bbox.getMaxY(), null);
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
            throws IOException {

        try {
            lockIndex.writeLock().lock();
            if (this.index == null) {
                this.index = new STRtree();
            }
            if (this.dirty || !isSubArea(query)) {
                // this.cachedFeatureSource.fillCache(query);
                integrateCache(query);
            }

            List features = index.query((Envelope) getEnvelope(query.getFilter()));

            return new FilteringFeatureReader(new SimpleListReader(features, getSchema()),
                    query.getFilter());

        } catch (Exception e) {
            throw new DataSourceException(
                    "Error occurred extracting features from the spatial index", e);
        } finally {
            lockIndex.writeLock().unlock();
        }
    }

}
