package edu.auth.jetproud.proud.partitioning.gridresolvers;

import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.partitioning.GridPartitioning;
import edu.auth.jetproud.utils.Lazy;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Parser;
import edu.auth.jetproud.utils.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class TAOGridPartitioner implements GridPartitioning.GridPartitioner
{
    private static final String delimiter = ";";

    private static final String pointsLiteral1D = "-0.01";
    private static final String pointsLiteral2D = "79.23;82.47;85.77";
    private static final String pointsLiteral3D = "26.932";

    private static Lazy<HashMap<Integer, ArrayList<Double>>> spatialTAO = new Lazy<>(()-> {
        HashMap<Integer, ArrayList<Double>> value = new HashMap<>();

        List<Tuple<Integer, String>> dimensionDataPairs = Lists.of(
                new Tuple<>(1, TAOGridPartitioner.pointsLiteral1D),
                new Tuple<>(2, TAOGridPartitioner.pointsLiteral2D),
                new Tuple<>(3, TAOGridPartitioner.pointsLiteral3D)
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

    public TAOGridPartitioner() {
        this.proudContext = null;
    }

    @Override
    public void setContext(ProudContext proudContext) {
        this.proudContext = proudContext;
    }

    @Override
    public GridPartitioning.PartitionNeighbourhood neighbourhoodOf(AnyProudData dataPoint, double range) {
        List<Double> points1d = spatialTAO.value().get(1);
        List<Double> points2d = spatialTAO.value().get(2);
        List<Double> points3d = spatialTAO.value().get(3);

        List<Integer> matchingPartitions = Lists.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);
        List<Integer> neighbours = Lists.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);

        List<Double> value = dataPoint.value;

        ////// Dimension 1
        if (value.get(0) <= points1d.get(0)) { //it belongs to x1 (1,2,3,4,5,6,7,8)
            matchingPartitions.removeAll(Lists.of(9, 10, 11, 12, 13, 14, 15, 16));

            if (value.get(0) >= points1d.get(0) - range) { //belongs to x2 too
                //nothing to do
            } else { //does not belong to x2
                neighbours.removeAll(Lists.of(9, 10, 11, 12, 13, 14, 15, 16));
            }

        } else { //it belongs to x2 (9,10,11,12,13,14,15,16)
            matchingPartitions.removeAll(Lists.of(1, 2, 3, 4, 5, 6, 7, 8));

            if (value.get(0) <= points1d.get(0) + range) { //belongs to x1 too
                //nothing to do
            } else {
                //does not belong to x1
                neighbours.removeAll(Lists.of(1, 2, 3, 4, 5, 6, 7, 8));
            }
        }

        ////// Dimension 2

        if (value.get(1) <= points2d.get(0)) { //it belongs to y1 (1,5,9,13)
            matchingPartitions.removeAll(Lists.of(2, 6, 10, 14, 3, 7, 11, 15, 4, 8, 12, 16));
            neighbours.removeAll(Lists.of(3, 7, 11, 15, 4, 8, 12, 16)); //y3 and y4 are not neighbors

            if (value.get(1) >= points2d.get(0) - range) { //belongs to y2 too
                //nothing to do
            } else {
                neighbours.removeAll(Lists.of(2, 6, 10, 14));
            }

        } else if (value.get(1) <= points2d.get(1)) { //it belongs to y2 (2,6,10,14)
            matchingPartitions.removeAll(Lists.of(1, 5, 9, 13, 3, 7, 11, 15, 4, 8, 12, 16));
            neighbours.removeAll(Lists.of(4, 8, 12, 16)); //y4 is not neighbor

            if (value.get(1) <= points2d.get(0) + range) { //belongs to y1 too
                neighbours.removeAll(Lists.of(3, 7, 11, 15)); //y3 is not neighbor
            } else if (value.get(1) >= points2d.get(1) - range) { //belongs to y3 too
                neighbours.removeAll(Lists.of(1, 5, 9, 13)); //y1 is not neighbor
            } else {
                //y1 and y3 are not neighbors
                neighbours.removeAll(Lists.of(1, 5, 9, 13, 3, 7, 11, 15));
            }
        } else if (value.get(1) <= points2d.get(2)) { //it belongs to y3 (3,7,11,15)
            matchingPartitions.removeAll(Lists.of(1, 5, 9, 13, 2, 6, 10, 14, 4, 8, 12, 16));
            neighbours.removeAll(Lists.of(1, 5, 9, 13)); //y1 is not neighbor

            if (value.get(1) <= points2d.get(1) + range) { //belongs to y2 too
                neighbours.removeAll(Lists.of(4, 8, 12, 16)); //y4 is not neighbor
            } else if (value.get(1) >= points2d.get(2) - range) { //belongs to y4 too
                neighbours.removeAll(Lists.of(2, 6, 10, 14)); //y2 is not neighbor
            } else {
                //y2 and y4 are not neighbors
                neighbours.removeAll(Lists.of(2, 6, 10, 14, 4, 8, 12, 16));
            }
        } else { //it belongs to y4 (4,8,12,16)
            matchingPartitions.removeAll(Lists.of(1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11, 15));
            neighbours.removeAll(Lists.of(1, 5, 9, 13, 2, 6, 10, 14)); //y1 and y2 are not neighbors

            if (value.get(1) <= points2d.get(2) + range) { //belongs to y3 too
                //nothing to do
            } else { //does not belong to y3
                neighbours.removeAll(Lists.of(3, 7, 11, 15));
            }
        }

        ////// Dimension 3

        if (value.get(2) <= points3d.get(0)) { //it belongs to z1 (5,6,7,8,9,10,11,12)
            matchingPartitions.removeAll(Lists.of(1, 2, 3, 4, 13, 14, 15, 16));

            if (value.get(2) >= points3d.get(0) - range) { //belongs to z2 too
                //nothing to do
            } else { //does not belong to z2
                neighbours.removeAll(Lists.of(1, 2, 3, 4, 13, 14, 15, 16));
            }
        } else { //it belongs to z2 (1,2,3,4,13,14,15,16)
            matchingPartitions.removeAll(Lists.of(5, 6, 7, 8, 9, 10, 11, 12));

            if (value.get(2) <= points3d.get(0) + range) { //belongs to z1 too
                //nothing to do
            } else {
                //does not belong to z1
                neighbours.removeAll(Lists.of(5, 6, 7, 8, 9, 10, 11, 12));
            }
        }

        int partition = matchingPartitions.get(0);
        // Use this because remove does not always work properly with
        // int values due to overload issue in the remove function:
        // List::remove(int index), List::remove(Object value)
        neighbours.removeIf((it)-> it == partition);

        return new GridPartitioning.PartitionNeighbourhood(partition, neighbours);
    }
}
