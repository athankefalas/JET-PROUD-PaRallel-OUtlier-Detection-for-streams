package edu.auth.jetproud.datastructures.mtree.partition;

import edu.auth.jetproud.datastructures.mtree.distance.DistanceFunction;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Pair;

import java.util.*;

/**
 * An object with partitions a set of data into two sub-sets.
 *
 * @param <DATA> The type of the data on the sets.
 */
public interface PartitionFunction<DATA> {

    /**
     * Executes the partitioning.
     *
     * @param promoted The pair of data objects that will guide the partition
     *        process.
     * @param dataSet The original set of data objects to be partitioned.
     * @param distanceFunction A {@linkplain DistanceFunction distance function}
     *        to be used on the partitioning.
     * @return A pair of partition sub-sets. Each sub-set must correspond to one
     *         of the {@code promoted} data objects.
     */
    Pair<Set<DATA>> process(Pair<DATA> promoted, Set<DATA> dataSet, DistanceFunction<? super DATA> distanceFunction);

    /**
     * A {@linkplain PartitionFunction partition function} that tries to
     * distribute the data objects equally between the promoted data objects,
     * associating to each promoted data objects the nearest data objects.
     *
     * <p>The algorithm is roughly equivalent to this:
     * <pre>
     *     While dataSet is not Empty:
     *         X := The object in dataSet which is nearest to promoted.<b>first</b>
     *         Remove X from dataSet
     *         Add X to result.<b>first</b>
     *
     *         Y := The object in dataSet which is nearest to promoted.<b>second</b>
     *         Remove Y from dataSet
     *         Add Y to result.<b>second</b>
     *
     *     Return result
     * </pre>
     *
     * @see mtree.PartitionFunction#process(mtree.utils.Pair, java.util.Set, DistanceFunction)
     */
    static <DATA> PartitionFunction<DATA> balanced() {
        return new BalancedPartitioning<>();
    }
}
