package edu.auth.jetproud.datastructures.mtree.partition;

import edu.auth.jetproud.datastructures.mtree.distance.DistanceFunction;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Pair;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A {@linkplain PartitionFunction partition function} that tries to
 * distribute the data objects equally between the promoted data objects,
 * associating to each promoted data objects the nearest data objects.
 *
 * @param <DATA> The type of the data objects.
 */
public class BalancedPartitioning<DATA> implements PartitionFunction<DATA>
{

    /**
     * Processes the balanced partition.
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
     * @see mtree.PartitionFunction#process(mtree.utils.Pair, java.util.Set, mtree.DistanceFunction)
     */
    @Override
    public Pair<Set<DATA>> process(Pair<DATA> promoted, Set<DATA> dataSet, DistanceFunction<? super DATA> distanceFunction) {
        List<DATA> queue1 = Lists.copyOf(dataSet);
        // Sort by distance to the first promoted data
        queue1.sort(distanceFunction.nearestToComparator(promoted.first));

        List<DATA> queue2 = Lists.copyOf(dataSet);
        // Sort by distance to the second promoted data
        queue2.sort(distanceFunction.nearestToComparator(promoted.second));

        Pair<Set<DATA>> partitions = new Pair<>(new HashSet<>(), new HashSet<>());

        int index1 = 0;
        int index2 = 0;

        while(index1 < queue1.size()  ||  index2 != queue2.size()) {
            while(index1 < queue1.size()) {
                DATA data = queue1.get(index1++);
                if(!partitions.second.contains(data)) {
                    partitions.first.add(data);
                    break;
                }
            }

            while(index2 < queue2.size()) {
                DATA data = queue2.get(index2++);
                if(!partitions.first.contains(data)) {
                    partitions.second.add(data);
                    break;
                }
            }
        }

        return partitions;
    }
}
