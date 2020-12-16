package edu.auth.jetproud.proud.partitioning.gridresolvers;

import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.partitioning.GridPartitioning;
import edu.auth.jetproud.utils.*;

import java.util.*;
import java.util.stream.Collectors;

public class StockGridPartitioner implements GridPartitioning.GridPartitioner
{

    private static final String delimiter = ";";

    /// A sampled literal string that contains the 15 boundaries for the grid
    private static final String pointsLiteral1DSample
            = "87.231;94.222;96.5;97.633;98.5;99.25;99.897;100.37;101.16;102.13;103.18;104.25;105.25;106.65;109.75";

    /// A literal string that contains the 15 boundaries for the grid
    ///     extracted from the entire test dataset. This is used for testing
    //      an algorithm's sensitivity to partitioning errors.
    private static final String pointsLiteral1DFull =
            "17.93;25.8;33.67;41.54;49.41;57.27;65.14;73.01;80.88;88.75;96.62;104.49;112.35;120.22;128.09";


    /// Static definition of partitions

    private static final String pointsLiteral1D = pointsLiteral1DSample;

    private static final Lazy<HashMap<Integer, ArrayList<Double>>> spatialStock = new Lazy<>(()->{
        HashMap<Integer, ArrayList<Double>> value = new HashMap<>();

        List<Tuple<Integer, String>> dimensionDataPairs = Lists.of(
                new Tuple<>(1, StockGridPartitioner.pointsLiteral1D)
        );

        for(Tuple<Integer, String> dimensionDataPair:dimensionDataPairs) {
            int dimension = dimensionDataPair.first;
            String data = dimensionDataPair.second;

            String[] pointsStrArray = data.split(delimiter);
            List<Double> points = Arrays.stream(pointsStrArray)
                    .map(Parser.ofDouble()::parseString)
                    .collect(Collectors.toList());


            value.put(dimension, new ArrayList<>(points));
        }

        return value;
    });

    private static final Lazy<ArrayList<Tuple<Double, Double>>> partitionRanges = new Lazy<>(()->{
        List<Double> partitionBoundaries = spatialStock.value().get(1);
        List<Tuple<Double, Double>> partitions = Lists.make();

        for (int partitionIndex = 0; partitionIndex < partitionBoundaries.size(); partitionIndex++) {
            Double previousBoundary = Lists.getAtOrNull(partitionBoundaries, partitionIndex - 1);
            double boundary = partitionBoundaries.get(partitionIndex);

            double rangeStart = previousBoundary != null ? previousBoundary : Double.MIN_VALUE;

            Tuple<Double, Double> partition = new Tuple<>(rangeStart, boundary);
            partitions.add(partition);

            if (partitionIndex == partitionBoundaries.size() - 1) {
                partition = new Tuple<>(boundary, Double.MAX_VALUE);
                partitions.add(partition);
            }
        }

        return new ArrayList<>(partitions);
    });

    /// Partitioner Implementation

    private ProudContext proudContext;

    public StockGridPartitioner(){
        this.proudContext = null;
    }

    @Override
    public void setContext(ProudContext proudContext) {
        this.proudContext = proudContext;
    }

    @Override
    public GridPartitioning.PartitionNeighbourhood neighbourhoodOf(AnyProudData dataPoint, double range) {
        int parallelism = 16;

        if (proudContext != null) {
            parallelism = proudContext.internalConfiguration().getPartitions();
        }

        List<Double> partitionBoundaries = spatialStock.value().get(1);

        if(partitionBoundaries.size() != parallelism - 1) {
            throw ExceptionUtils.sneaky(
                    new IllegalArgumentException("The defined grid boundaries should be equal to the number of partitions - 1.")
            );
        }

        double point = dataPoint.value.get(0);

        List<Tuple<Double, Double>> partitions = partitionRanges.value();

        List<Integer> matchingPartitions = Lists.make();
        List<Integer> neighbours = Lists.make();

        for (int i=0;i<partitions.size();i++) {
            Tuple<Double, Double> previous = Lists.getAtOrNull(partitions, i - 1);
            Tuple<Double, Double> current = partitions.get(i);
            Tuple<Double, Double> next = Lists.getAtOrNull(partitions, i + 1);

            if (isInside(current, point)) {
                matchingPartitions.add(i);

                if (previous != null) {

                    for(int j=i-1; j >= 0; j--) {
                        Tuple<Double, Double> precedingPartition = partitions.get(j);

                        if (isInside(precedingPartition, point - range)) {
                            final int precedingPartitionIndex = j;
                            if (matchingPartitions.stream().noneMatch((p)->p == precedingPartitionIndex))
                                neighbours.add(j);
                        } else {
                            break;
                        }
                    }
                }

                if (next != null) {
                    for(int j=i+1; j<partitions.size(); j++) {
                        Tuple<Double, Double> proceedingPartition = partitions.get(j);

                        if (isInside(proceedingPartition, point + range)) {
                            final int proceedingPartitionIndex = j;
                            if (matchingPartitions.stream().noneMatch((p)->p == proceedingPartitionIndex))
                                neighbours.add(j);
                        } else {
                            break;
                        }
                    }
                }
            }

        }

        matchingPartitions = matchingPartitions.stream()
                .sorted()
                .distinct()
                .collect(Collectors.toList());

        neighbours = neighbours.stream()
                .sorted()
                .distinct()
                .collect(Collectors.toList());

        return new GridPartitioning.PartitionNeighbourhood(matchingPartitions, neighbours);
    }

    private boolean isInside(Tuple<Double,Double> partition, double point) {
        return point >= partition.first && point <= partition.second;
    }

    // Custom GRID partitioning - algorithms work kinda. BUT, partitioning is not correct !!!
    public GridPartitioning.PartitionNeighbourhood mineOLDNeighbourhoodOf(AnyProudData dataPoint, double range) {
        int parallelism = 16;

        if (proudContext != null) {
            parallelism = proudContext.internalConfiguration().getPartitions();
        }

        List<Double> partitionBoundaries = spatialStock.value().get(1);

        if(partitionBoundaries.size() != parallelism - 1) {
            throw ExceptionUtils.sneaky(
                    new IllegalArgumentException("The defined grid boundaries should be equal to the number of partitions - 1 ("+(parallelism-1)+").")
            );
        }

        List<Integer> neighbours = Lists.make();

        int partition = -1;
        int previousPartition = -1;
        int nextPartition = -1;

        double point = dataPoint.value.get(0);

        for (int partitionIndex = 0; partitionIndex < partitionBoundaries.size(); partitionIndex++) {
            Double previousBoundary = Lists.getAtOrNull(partitionBoundaries, partitionIndex - 1);
            double boundary = partitionBoundaries.get(partitionIndex);
            Double nextBoundary = Lists.getAtOrNull(partitionBoundaries, partitionIndex + 1);

            if (previousBoundary != null && nextBoundary != null) {
                if (point > previousBoundary && point <= boundary) {
                    partition = partitionIndex;

                    if (point >= previousBoundary + range)
                        previousPartition = partitionIndex - 1;

                    if (point >= nextBoundary - range)
                        nextPartition = partitionIndex + 1;

                    break;
                }

            } else if (previousBoundary != null) {
                if (point > previousBoundary && point <= boundary) {
                    partition = partitionIndex;

                    if (point >= previousBoundary + range)
                        previousPartition = partitionIndex - 1;

                    break;
                }
            } else if (nextBoundary != null) {
                if (point >= boundary) {
                    partition = partitionIndex;

                    if (point >= nextBoundary - range)
                        nextPartition = partitionIndex + 1;

                    break;
                }
            } else {
                partition = partitionIndex;
            }
        }

        // Add to neighbours if needed
        if (nextPartition != -1)
            neighbours.add(nextPartition);

        // Add to neighbours if needed
        if (previousPartition != -1)
            neighbours.add(previousPartition);

        return new GridPartitioning.PartitionNeighbourhood(Lists.of(partition), neighbours);
    }

    // Original Proud GRID partitioning method from flink Impl - does not work with PMCSky & SOP
    public GridPartitioning.PartitionNeighbourhood originalNeighbourhoodOf(AnyProudData dataPoint, double range) {
        int parallelism = 16;

        if (proudContext != null) {
            parallelism = proudContext.internalConfiguration().getPartitions();
        }

        List<Double> points = spatialStock.value().get(1);
        List<Integer> neighbours = Lists.make();

        List<Double> value = dataPoint.value;

        int i = 0;
        boolean placedInPartition = false;

        int matchingPartition = -1;
        int previous = -1;
        int next = -1;

        do {
            if (value.get(0) <= points.get(i)) {
                matchingPartition = i; //belongs to the current partition
                placedInPartition = true;

                if (i != 0) {
                    //check if it is near the previous partition
                    if (value.get(0) <= points.get(i - 1) + range) {
                        previous = i - 1;
                    }
                }

                //check if it is near the next partition
                if (value.get(0) >= points.get(i) - range) {
                    next = i + 1;
                }
            }

            i += 1;
        } while (i <= parallelism - 2 && !placedInPartition);

        if (!placedInPartition) {
            // it belongs to the last partition
            matchingPartition = parallelism - 1;

            if (value.get(0) <= points.get(parallelism - 2) + range) {
                previous = parallelism - 2;
            }
        }

        int partition = matchingPartition;

        // Add to neighbours if needed
        if (next != -1)
            neighbours.add(next);

        // Add to neighbours if needed
        if (previous != -1)
            neighbours.add(previous);

        neighbours = neighbours.stream()
                .sorted()
                .distinct()
                .collect(Collectors.toList());

        return new GridPartitioning.PartitionNeighbourhood(Lists.of(partition), neighbours);
    }


}
