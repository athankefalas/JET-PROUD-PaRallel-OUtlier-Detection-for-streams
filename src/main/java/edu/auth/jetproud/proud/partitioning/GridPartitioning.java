package edu.auth.jetproud.proud.partitioning;

import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.utils.Lists;

import java.io.Serializable;
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

    public interface GridPartitioner extends Serializable
    {
        default void setContext(ProudContext proudContext) {
            // Nothing here, implement if access to proud ctx is needed
        }

        PartitionNeighbourhood neighbourhoodOf(AnyProudData dataPoint, double range);
    }

    private ProudContext proudContext;

    private int partitionsCount;
    private double range;

    private GridPartitioner gridPartitioner;

    private GridPartitioning() {
        this(null,-1, 0, null);
    }

    public GridPartitioning(ProudContext proudContext, GridPartitioner gridPartitioner) {
        this(proudContext, proudContext.internalConfiguration().getPartitions(), proudContext.internalConfiguration().getCommonR(), gridPartitioner);
    }

    public GridPartitioning(ProudContext proudContext, int partitionsCount, double range, GridPartitioner gridPartitioner) {
        this.proudContext = proudContext;

        this.partitionsCount = partitionsCount;
        this.range = range;
        this.gridPartitioner = gridPartitioner;
    }

    @Override
    public List<PartitionedData<AnyProudData>> partition(AnyProudData dataPoint) {
        // Set the proud context to the partitioner
        gridPartitioner.setContext(proudContext);

        PartitionNeighbourhood dataNeighbourhood = gridPartitioner.neighbourhoodOf(dataPoint, range);
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
