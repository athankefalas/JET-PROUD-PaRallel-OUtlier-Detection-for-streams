package edu.auth.jetproud.proud.partitioning.custom;

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.partitioning.PartitionedData;
import edu.auth.jetproud.proud.partitioning.ProudPartitioning;
import edu.auth.jetproud.utils.ExceptionUtils;

import java.util.List;

public final class CustomFunctionProudPartitioning  implements ProudPartitioning
{
    private ProudContext proudContext;
    private BiConsumerEx<AnyProudData, PartitioningState> partitionFunction;

    public CustomFunctionProudPartitioning(ProudContext proudContext, BiConsumerEx<AnyProudData, PartitioningState> partitionFunction) {
        this.proudContext = proudContext;
        this.partitionFunction = partitionFunction;
    }

    @Override
    public final List<PartitionedData<AnyProudData>> partition(AnyProudData dataPoint) {
        PartitioningState partitioningState = new PartitioningState(proudContext);

        try {
            partitionFunction.acceptEx(dataPoint, partitioningState);
        } catch (Exception e) {
            throw ExceptionUtils.sneaky(e);
        }

        return partitioningState.getData();
    }


    @Override
    public final Traverser<PartitionedData<AnyProudData>> jetPartition(AnyProudData dataPoint) {
        List<PartitionedData<AnyProudData>> partitions = partition(dataPoint);
        return Traversers.traverseIterable(partitions);
    }
}
