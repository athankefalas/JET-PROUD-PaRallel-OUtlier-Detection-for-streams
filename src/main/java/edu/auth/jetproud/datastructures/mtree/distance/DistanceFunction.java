package edu.auth.jetproud.datastructures.mtree.distance;


import edu.auth.jetproud.datastructures.mtree.MTree;
import edu.auth.jetproud.datastructures.mtree.split.SplitFunction;
import edu.auth.jetproud.model.contracts.EuclideanCoordinate;
import edu.auth.jetproud.utils.EuclideanCoordinateList;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An object that can calculate the distance between two data objects.
 *
 * @param <DATA> The type of the data objects.
 */
public interface DistanceFunction<DATA> extends Serializable {

    /**
     * Calculates the distance between two data points.
     * @return The points distance.
     */
    double calculate(DATA data1, DATA data2);

    default Comparator<DATA> nearestToComparator(DATA other) {
        return (data1, data2) -> {
            double distance1 = calculate(data1, other);
            double distance2 = calculate(data2, other);
            return Double.compare(distance1, distance2);
        };
    }

    /**
     * Creates a cached version of a {@linkplain DistanceFunction distance
     * function}. This method is used internally by {@link MTree} to create
     * a cached distance function to pass to the {@linkplain SplitFunction split
     * function}.
     * @param distanceFunction The distance function to create a cached version
     *        of.
     * @return The cached distance function.
     */
    static <DATA> DistanceFunction<DATA> cached(DistanceFunction<DATA> distanceFunction) {
        return new DistanceFunction<DATA>() {

            private final Map<Integer, Double> cache = new HashMap<>();
            private final DistanceFunction<DATA> delegatingDistanceFunction = distanceFunction;

            /**
             *   Create a key that represents the two elements.
             *   Argument order is independent.
             */
            private Integer keyFor(DATA data1, DATA data2) {
                return data1.hashCode() ^ data2.hashCode();
            }

            @Override
            public double calculate(DATA data1, DATA data2) {
                Integer key = keyFor(data1, data2);

                if (cache.containsKey(key))
                    return cache.get(key);

                double distance = delegatingDistanceFunction.calculate(data1, data2);
                cache.put(key, distance);

                return distance;
            }
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

            return euclideanDistance.calculate(data1, data2);
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

