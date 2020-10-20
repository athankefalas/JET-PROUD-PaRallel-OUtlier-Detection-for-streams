package edu.auth.jetproud.datastructures.vptree.distance;

import edu.auth.jetproud.model.contracts.EuclideanCoordinate;
import edu.auth.jetproud.utils.EuclideanCoordinateList;

import java.util.Comparator;
import java.util.List;

/**
 * <p>A function that calculates the distance between two points. For the purposes of vp-trees, distance functions must
 * conform to the rules of a metric space, namely:</p>
 *
 * <ol>
 *  <li>d(x, y) &ge; 0</li>
 *  <li>d(x, y) = 0 if and only if x = y</li>
 *  <li>d(x, y) = d(y, x)</li>
 *  <li>d(x, z) &le; d(x, y) + d(y, z)</li>
 * </ol>
 *
 * @author <a href="https://github.com/jchambers">Jon Chambers</a>
 */
public interface DistanceFunction<T> {

    /**
     * Returns the distance between two points.
     *
     * @param firstPoint the first point
     * @param secondPoint the second point
     *
     * @return the distance between the two points
     */
    double getDistance(T firstPoint, T secondPoint);

    default Comparator<T> nearestToComparator(T other) {
        return (data1, data2) -> {
            double distance1 = getDistance(data1, other);
            double distance2 = getDistance(data2, other);
            return Double.compare(distance1, distance2);
        };
    }

    /**
     * Calculates the distance between two {@linkplain EuclideanCoordinate
     * euclidean coordinates}.
     */
    static <DATA extends EuclideanCoordinate> DistanceFunction<DATA> euclidean() {
        return (data1, data2) -> {
            int dimensions = Math.min(data1.dimensions(), data2.dimensions());
            double dimensionDeltaSquaredSum = 0;

            for(int d = 0; d < dimensions; d++) {
                double diff = data1.get(d) - data2.get(d);
                dimensionDeltaSquaredSum += diff * diff;
            }

            return Math.sqrt(dimensionDeltaSquaredSum);
        };
    }

    /**
     * Calculates the distance between two List<E> instances after converting them to {@linkplain EuclideanCoordinate
     * euclidean coordinates}.
     * @param <E> The type of the list element. Must extends Number.
     */
    static <DATA extends List<E>, E extends Number> DistanceFunction<DATA> euclideanFromList() {
        final DistanceFunction<EuclideanCoordinate> euclideanDistance = DistanceFunction.euclidean();

        return (list1, list2) -> {
            EuclideanCoordinate data1 = new EuclideanCoordinateList<>(list1);
            EuclideanCoordinate data2 = new EuclideanCoordinateList<>(list2);

            return euclideanDistance.getDistance(data1, data2);
        };
    }

    /**
     * Calculates the distance between two List<E> instances after converting them to {@linkplain EuclideanCoordinate
     * euclidean coordinates}.
     * @param numberType The class of the list elements. Used only to resolve type parameters.
     * @param <E> The type of the list element. Must extends Number.
     */
    static <DATA extends List<E>, E extends Number> DistanceFunction<DATA> euclideanFromListOf(Class<? extends E> numberType) {
        return euclideanFromList();
    }
}
