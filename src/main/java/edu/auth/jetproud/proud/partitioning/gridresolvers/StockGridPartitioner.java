package edu.auth.jetproud.proud.partitioning.gridresolvers;

import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.partitioning.GridPartitioning;
import edu.auth.jetproud.utils.Lazy;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Parser;
import edu.auth.jetproud.utils.Tuple;

import java.util.*;
import java.util.stream.Collectors;

public class StockGridPartitioner implements GridPartitioning.GridPartitioner
{

    private static final String delimiter = ";";
    private static final String pointsLiteral1D
            = "87.231;94.222;96.5;97.633;98.5;99.25;99.897;100.37;101.16;102.13;103.18;104.25;105.25;106.65;109.75";

    private static Lazy<HashMap<Integer, ArrayList<Double>>> spatialStock = new Lazy<>(()->{
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

        return new GridPartitioning.PartitionNeighbourhood(partition, neighbours);
    }

}
