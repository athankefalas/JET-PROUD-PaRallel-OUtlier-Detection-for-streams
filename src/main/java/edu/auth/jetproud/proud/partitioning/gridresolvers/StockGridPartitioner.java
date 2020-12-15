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
    private static final String pointsLiteral1D
            = "87.231;94.222;96.5;97.633;98.5;99.25;99.897;100.37;101.16;102.13;103.18;104.25;105.25;106.65;109.75";

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

        return new GridPartitioning.PartitionNeighbourhood(partition, neighbours);
    }

}
