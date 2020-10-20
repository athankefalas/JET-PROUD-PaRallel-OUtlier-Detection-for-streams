package edu.auth.jetproud.proud.partitioning;

import edu.auth.jetproud.utils.Parser;
import edu.auth.jetproud.datastructures.vptree.VPTree;
import edu.auth.jetproud.datastructures.vptree.distance.DistanceFunction;
import edu.auth.jetproud.exceptions.ProudException;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.partitioning.exceptions.ProudPartitioningException;
import edu.auth.jetproud.utils.Lists;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TreePartitioning implements ProudPartitioning
{
    private int treeInitElements;
    private int partitionsCount;
    private double radius;

    private String initFilePath;
    private VPTree<AnyProudData, AnyProudData> tree;

    private TreePartitioning() throws ProudException {
        this(0,0,0,"");
    }

    public TreePartitioning(int treeInitElements, int partitionsCount, double radius, String initFilePath) throws ProudException {
        this.treeInitElements = treeInitElements;
        this.partitionsCount = partitionsCount;
        this.initFilePath = initFilePath;

        this.radius = radius;

        // Create and initialize VP tree
        this.tree = createVantagePointTree();
        this.tree.createPartitions(partitionsCount);
    }

    private VPTree<AnyProudData, AnyProudData> createVantagePointTree() throws ProudException {
        List<String> lines = Lists.make();

        try {
            FileReader fileReader = new FileReader(initFilePath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line = "";

            while (line != null) {
                line = bufferedReader.readLine();
                lines.add(line);

                if (lines.size() >= treeInitElements) {
                    break;
                }
            }

            bufferedReader.close();
            fileReader.close();
        } catch (IOException e) {
            throw ProudPartitioningException.dataSampleError(e);
        }

        if (lines.isEmpty()) {
            throw ProudPartitioningException.emptyDataSampleError();
        }

        Parser<List<Double>> dataPointParser = Parser.ofDoubleList(",");

        List<List<Double>> dataPoints = lines.stream()
                .map(dataPointParser::parseString)
                .collect(Collectors.toList());

        int unreadableDataPointLineIndex = -1;

        for (int i=0; i < dataPoints.size(); i++) {
            List<Double> points = dataPoints.get(i);

            if (points.stream().anyMatch(Objects::isNull)) {
                unreadableDataPointLineIndex = i;
                break;
            }
        }

        if (unreadableDataPointLineIndex != -1) {
            throw ProudPartitioningException
                    .dataSampleParseError(unreadableDataPointLineIndex + 1);
        }

        List<AnyProudData> data = dataPoints.stream()
                .map((it)-> new AnyProudData(0, it, 0, 0))
                .collect(Collectors.toList());

        return new VPTree<>(DistanceFunction.euclidean(), data);
    }


    @Override
    public List<PartitionedData<AnyProudData>> partition(AnyProudData dataPoint) {
        List<PartitionedData<AnyProudData>> dataPartitions = Lists.make();

        List<Integer> partitions = Lists.make();

        // Find partitions from VPTree
        List<String> partitionsString = tree.findPartitions(dataPoint, radius, partitionsCount);
        List<String> positivePartitionsString = partitionsString.stream()
                .filter((it) -> it.contains("true"))
                .collect(Collectors.toList());

        int trueCount = positivePartitionsString.size();

        if (trueCount != 1) {
            ProudPartitioningException error
                    = ProudPartitioningException.internalPartitioningError("VP Tree partitioning error.");
            throw new RuntimeException(error);
        }

        // Parse using filter.contains("true") - i.e. partition point belongs to
        String firstMatch = positivePartitionsString.stream()
                .findFirst()
                .orElse(null);

        if (firstMatch == null) {
            ProudPartitioningException error
                    = ProudPartitioningException.internalPartitioningError("No positive partition found from VP Tree.");
            throw new RuntimeException(error);
        }

        Parser<Integer> integerParser = Parser.ofInt();
        Integer positivePartition = Arrays.stream(firstMatch.split("&"))
                .map(integerParser::parseString)
                .findFirst()
                .orElse(null);

        if (positivePartition == null) {
            ProudPartitioningException error
                    = ProudPartitioningException.internalPartitioningError("No positive partition found from VP Tree.");
            throw new RuntimeException(error);
        }

        partitions.add(positivePartition);

        // Parse using filter.contains("false") - i.e. neighbouring partitions point belongs to
        List<Integer> negativePartitions = partitionsString.stream()
                .filter((it) -> it.contains("false"))
                .map((it)->it.split("&")[0])
                .map(integerParser::parseString)
                .collect(Collectors.toList());

        if (negativePartitions.stream().anyMatch(Objects::isNull)) {
            ProudPartitioningException error
                    = ProudPartitioningException.internalPartitioningError("Null negative partitions found in parsed VP Tree partitions.");
            throw new RuntimeException(error);
        }

        partitions.addAll(negativePartitions);

        // Create Data Partitions
        PartitionedData<AnyProudData> partitionedData = new PartitionedData<>(0, dataPoint);
        dataPartitions.add(partitionedData);

        if (partitions.size() > 1) {
            for (int i=1; i < partitions.size(); i++) {
                int partitionNeighbour = partitions.get(i);

                AnyProudData dataPointCopy = new AnyProudData(dataPoint.id, dataPoint.value, dataPoint.arrival,  1);
                partitionedData = new PartitionedData<>(partitionNeighbour, dataPointCopy);

                dataPartitions.add(partitionedData);
            }
        }

        return dataPartitions;
    }
}
