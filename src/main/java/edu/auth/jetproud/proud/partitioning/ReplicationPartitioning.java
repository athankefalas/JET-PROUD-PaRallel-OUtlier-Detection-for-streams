package edu.auth.jetproud.proud.partitioning;

import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.utils.Lists;

import java.util.List;

public class ReplicationPartitioning implements ProudPartitioning
{

    private ProudContext proudContext;
    private int partitionsCount;

    private ReplicationPartitioning() {
        this(null,0);
    }

    public ReplicationPartitioning(ProudContext proudContext) {
        this(proudContext, proudContext.internalConfiguration().getPartitions());
    }

    public ReplicationPartitioning(ProudContext proudContext, int partitionsCount) {
        this.proudContext = proudContext;
        this.partitionsCount = partitionsCount;
    }

    public ReplicationPartitioning(int partitionsCount) {
        this.partitionsCount = partitionsCount;
    }

    @Override
    public List<PartitionedData<AnyProudData>> partition(AnyProudData dataPoint) {
        List<PartitionedData<AnyProudData>> dataPartitions = Lists.make();

        for (int p=0; p < partitionsCount; p++) {
            if (dataPoint.id % partitionsCount == p) {
                AnyProudData dataPointCopy = new AnyProudData(dataPoint.id, dataPoint.value, dataPoint.arrival,  0);
                PartitionedData<AnyProudData> partitionedData = new PartitionedData<>(p, dataPoint);

                dataPartitions.add(partitionedData);
            } else {
                AnyProudData dataPointCopy = new AnyProudData(dataPoint.id, dataPoint.value, dataPoint.arrival,  1);
                PartitionedData<AnyProudData> partitionedData = new PartitionedData<>(p, dataPointCopy);

                dataPartitions.add(partitionedData);
            }
        }

        return dataPartitions;
    }
}
