package edu.auth.jetproud.proud.partitioning;

import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.utils.Lists;

import java.io.Serializable;
import java.util.List;

public class GridPartitioning implements ProudPartitioning
{
    private static final boolean USE_ONLY_PRIMARY_PARTITION = true;

    public static class PartitionNeighbourhood
    {
        private List<Integer> partitions;
        private List<Integer> neighbours;

        public PartitionNeighbourhood() {
            this(Lists.make(), Lists.make());
        }

        public PartitionNeighbourhood(List<Integer> partitions, List<Integer> neighbours) {
            this.partitions = partitions;
            this.neighbours = neighbours;
        }

        public List<Integer> getPartitions() {
            return partitions;
        }

        public void setPartition(List<Integer> partitions) {
            this.partitions = partitions;
        }

        public List<Integer> getNeighbours() {
            return neighbours;
        }

        public void setNeighbours(List<Integer> neighbours) {
            this.neighbours = neighbours;
        }

        @Override
        public String toString() {
            return "PartitionNeighbourhood{" +
                    "partition=" + partitions +
                    ", neighbours=" + neighbours +
                    '}';
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

        if (!dataNeighbourhood.getPartitions().isEmpty()) {
            int primaryPartition = dataNeighbourhood.getPartitions().get(0);
            AnyProudData dataPointCopy = new AnyProudData(dataPoint.id, dataPoint.value, dataPoint.arrival,  0);
            PartitionedData<AnyProudData> partitionedData = new PartitionedData<>(primaryPartition, dataPointCopy);
            dataPartitions.add(partitionedData);

            if (dataNeighbourhood.getPartitions().size() > 1) {

                for (int i=1; i<dataNeighbourhood.getPartitions().size(); i++) {
                    final int partition = dataNeighbourhood.getPartitions().get(i);

                    if (!USE_ONLY_PRIMARY_PARTITION) {
                        dataPointCopy = new AnyProudData(dataPoint.id, dataPoint.value, dataPoint.arrival,  0);
                        partitionedData = new PartitionedData<>(partition, dataPointCopy);
                    } else {
                        if (dataNeighbourhood.getNeighbours().stream().anyMatch((it)->it == partition))
                            continue;

                        dataPointCopy = new AnyProudData(dataPoint.id, dataPoint.value, dataPoint.arrival,  1);
                        partitionedData = new PartitionedData<>(partition, dataPointCopy);
                    }

                    dataPartitions.add(partitionedData);
                }

            }
        }

        for (Integer neighbouringPartition : dataNeighbourhood.neighbours) {
            AnyProudData dataPointCopy = new AnyProudData(dataPoint.id, dataPoint.value, dataPoint.arrival,  1);
            PartitionedData<AnyProudData> partitionedData = new PartitionedData<>(neighbouringPartition, dataPointCopy);

            dataPartitions.add(partitionedData);
        }

        return dataPartitions;
    }
}
