package edu.auth.jetproud.datastructures.mtree.promotion;

import edu.auth.jetproud.datastructures.mtree.distance.DistanceFunction;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.utils.CollectionUtils;
import edu.auth.jetproud.utils.Pair;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An object that chooses a pair from a set of data objects.
 *
 * @param <DATA> The type of the data objects.
 */
public interface PromotionFunction<DATA> {

    /**
     * Chooses (promotes) a pair of objects according to some criteria that is
     * suitable for the application using the M-Tree.
     *
     * @param dataSet The set of objects to choose a pair from.
     * @param distanceFunction A function that can be used for choosing the
     *        promoted objects.
     * @return A pair of chosen objects.
     */
    Pair<DATA> process(Set<DATA> dataSet, DistanceFunction<? super DATA> distanceFunction);


    /**
     * A {@linkplain PromotionFunction promotion function} object that randomly
     * chooses ("promotes") two data objects.
     *
     * @param <DATA> The type of the data objects.
     */
    static <DATA> PromotionFunction<DATA> random() {
        return (data, distanceFunction) -> {
            List<DATA> promotedList = CollectionUtils.randomSample(data, 2);
            return new Pair<>(promotedList.get(0), promotedList.get(1));
        };
    }


    /**
     * A {@linkplain PromotionFunction promotion function} object that randomly
     * chooses ("promotes") two data objects.
     *
     * @param <DATA> The type of the data objects.
     */
    static <DATA extends Comparable> PromotionFunction<DATA> minMax() {
        return (data, distanceFunction) -> CollectionUtils.minMax(data);
    }



}
