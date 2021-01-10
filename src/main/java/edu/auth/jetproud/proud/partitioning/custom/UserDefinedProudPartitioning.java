package edu.auth.jetproud.proud.partitioning.custom;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.partitioning.PartitionedData;
import edu.auth.jetproud.proud.partitioning.ProudPartitioning;

import java.util.List;

public abstract class UserDefinedProudPartitioning implements ProudPartitioning
{
    protected ProudContext proudContext;

    public UserDefinedProudPartitioning(ProudContext proudContext) {
        this.proudContext = proudContext;
    }

    @Override
    public final List<PartitionedData<AnyProudData>> partition(AnyProudData dataPoint) {
        PartitioningState partitioningState = new PartitioningState(proudContext);
        partitionPoint(dataPoint, partitioningState);

        return partitioningState.getData();
    }

    abstract void partitionPoint(AnyProudData dataPoint, PartitioningState partitioningState);

    @Override
    public final Traverser<PartitionedData<AnyProudData>> jetPartition(AnyProudData dataPoint) {
        List<PartitionedData<AnyProudData>> partitions = partition(dataPoint);
        return Traversers.traverseIterable(partitions);
    }
}
