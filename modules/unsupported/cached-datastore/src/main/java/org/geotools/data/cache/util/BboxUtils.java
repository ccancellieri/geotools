package org.geotools.data.cache.util;

import com.vividsolutions.jts.geom.Envelope;

public abstract class BboxUtils {

    // check the type of requested BBOX
    // |===|=========================|=====
    // | 1 |..........2..............|..3..
    // |===X=========================X=====
    // | . |.........................|.....
    // | 4 |.........bbox............|..5..
    // | . |.........................|.....
    // |===X=========================X=====
    // | 6 |..........7..............|..8..
    // |===|=========================|=====

    public static Envelope topLeft(Envelope cachedBbox, Envelope bbox) {
        return new Envelope(cachedBbox.getMinX(), bbox.getMaxY(), bbox.getMinX(),
                cachedBbox.getMaxY());
    }

    public static Envelope topCenter(Envelope cachedBbox, Envelope bbox) {
        return new Envelope(bbox.getMinX(), bbox.getMaxY(), bbox.getMaxX(), cachedBbox.getMaxY());
    }

    public static Envelope topRight(Envelope cachedBbox, Envelope bbox) {
        return new Envelope(bbox.getMaxX(), bbox.getMaxY(), cachedBbox.getMaxX(),
                cachedBbox.getMaxY());
    }
    
    public static Envelope centerLeft(Envelope cachedBbox, Envelope bbox) {
        return new Envelope(cachedBbox.getMinX(), bbox.getMinY(), bbox.getMinX(), bbox.getMaxY());
    }

    public static Envelope centerRight(Envelope cachedBbox, Envelope bbox) {
        return new Envelope(bbox.getMaxX(), bbox.getMinY(), cachedBbox.getMaxX(), bbox.getMaxY());
    }

    public static Envelope bottomLeft(Envelope cachedBbox, Envelope bbox) {
        return new Envelope(cachedBbox.getMinX(), cachedBbox.getMinY(), bbox.getMinX(),
                bbox.getMinY());
    }

    public static Envelope bottomCenter(Envelope cachedBbox, Envelope bbox) {
        return new Envelope(bbox.getMinX(), cachedBbox.getMinY(), bbox.getMaxX(), bbox.getMinY());
    }

    public static Envelope bottomRight(Envelope cachedBbox, Envelope bbox) {
        return new Envelope(bbox.getMaxX(), cachedBbox.getMinY(), cachedBbox.getMaxX(),
                bbox.getMinY());
    }

    // 1(TL) 2(TC) 3(TR) 4(CL) 5(CR) 6(BL) 7(BC) 8(BR)

    // TOP

    public static boolean isTopLeft(Envelope cachedBbox, Envelope bbox) {
        return isTop(cachedBbox, bbox) && isLeft(cachedBbox, bbox);
    }

    public static boolean isTopRight(Envelope cachedBbox, Envelope bbox) {
        return isTop(cachedBbox, bbox) && isRight(cachedBbox, bbox);
    }

    public static boolean isTopCenter(Envelope cachedBbox, Envelope bbox) {
        return isTop(cachedBbox, bbox) && !isRight(cachedBbox, bbox) && !isLeft(cachedBbox, bbox);
    }

    // BOTTOM

    public static boolean isBottomLeft(Envelope cachedBbox, Envelope bbox) {
        return isBottom(cachedBbox, bbox) && isLeft(cachedBbox, bbox);
    }

    public static boolean isBottomRight(Envelope cachedBbox, Envelope bbox) {
        return isBottom(cachedBbox, bbox) && isRight(cachedBbox, bbox);
    }

    public static boolean isBottomCenter(Envelope cachedBbox, Envelope bbox) {
        return isBottom(cachedBbox, bbox) && !isRight(cachedBbox, bbox)
                && !isLeft(cachedBbox, bbox);
    }

    // CENTER

    public static boolean isCenterLeft(Envelope cachedBbox, Envelope bbox) {
        return !isBottom(cachedBbox, bbox) && !isTop(cachedBbox, bbox) && isLeft(cachedBbox, bbox);
    }
    
    public static boolean isCenterRight(Envelope cachedBbox, Envelope bbox) {
        return !isBottom(cachedBbox, bbox) && !isTop(cachedBbox, bbox) && isRight(cachedBbox, bbox);
    }

    // SIDES

    public static boolean isTop(Envelope cachedBbox, Envelope bbox) {
        return cachedBbox.getMaxY() > bbox.getMaxY();
    }

    public static boolean isBottom(Envelope cachedBbox, Envelope bbox) {
        return cachedBbox.getMinY() < bbox.getMinY();
    }

    public static boolean isLeft(Envelope cachedBbox, Envelope bbox) {
        return cachedBbox.getMinX() < bbox.getMinX();
    }

    public static boolean isRight(Envelope cachedBbox, Envelope bbox) {
        return cachedBbox.getMaxX() > bbox.getMaxX();
    }

}
