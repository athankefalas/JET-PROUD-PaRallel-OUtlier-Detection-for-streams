package edu.auth.jetproud.proud.partitioning;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AppendableTraverser;
import edu.auth.jetproud.model.AnyProudData;

import java.io.Serializable;
import java.util.List;

public interface ProudPartitioning extends Serializable
{
    List<PartitionedData<AnyProudData>> partition(AnyProudData dataPoint);

    default Traverser<PartitionedData<AnyProudData>> jetPartition(AnyProudData dataPoint) {
        List<PartitionedData<AnyProudData>> partitions = partition(dataPoint);
        return Traversers.traverseIterable(partitions);
    }
}
