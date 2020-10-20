package edu.auth.jetproud.proud.partitioning;

import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.utils.Lists;

import java.util.List;

public class GridPartitioning implements ProudPartitioning
{
    public static class PartitionNeighbourhood
    {
        private int partition;
        private List<Integer> neighbours;

        public PartitionNeighbourhood() {
            this(-1, Lists.make());
        }

        public PartitionNeighbourhood(int partition, List<Integer> neighbours) {
            this.partition = partition;
            this.neighbours = neighbours;
        }

        public int getPartition() {
            return partition;
        }

        public void setPartition(int partition) {
            this.partition = partition;
        }

        public List<Integer> getNeighbours() {
            return neighbours;
        }

        public void setNeighbours(List<Integer> neighbours) {
            this.neighbours = neighbours;
        }
    }

    public interface PartitionNeighbourhoodResolver
    {
        PartitionNeighbourhood neighbourhoodOf(AnyProudData dataPoint, double range);
    }

    private int partitionsCount;
    private double range;

    private PartitionNeighbourhoodResolver partitionNeighbourhoodResolver;

    private GridPartitioning() {
        this(-1, 0, null);
    }

    public GridPartitioning(int partitionsCount, double range, PartitionNeighbourhoodResolver partitionNeighbourhoodResolver) {
        this.partitionsCount = partitionsCount;
        this.range = range;
        this.partitionNeighbourhoodResolver = partitionNeighbourhoodResolver;
    }

    @Override
    public List<PartitionedData<AnyProudData>> partition(AnyProudData dataPoint) {
        PartitionNeighbourhood dataNeighbourhood = partitionNeighbourhoodResolver.neighbourhoodOf(dataPoint, range);
        List<PartitionedData<AnyProudData>> dataPartitions = Lists.make();

        PartitionedData<AnyProudData> partitionedData = new PartitionedData<>(dataNeighbourhood.partition, dataPoint);
        dataPartitions.add(partitionedData);

        for (Integer neighbouringPartition : dataNeighbourhood.neighbours) {
            AnyProudData dataPointCopy = new AnyProudData(dataPoint.id, dataPoint.value, dataPoint.arrival,  1);
            partitionedData = new PartitionedData<>(neighbouringPartition, dataPointCopy);

            dataPartitions.add(partitionedData);
        }

        return dataPartitions;
    }
}
