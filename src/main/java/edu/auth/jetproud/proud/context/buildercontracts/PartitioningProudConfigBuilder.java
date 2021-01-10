package edu.auth.jetproud.proud.context.buildercontracts;

import edu.auth.jetproud.proud.partitioning.GridPartitioning;

public interface PartitioningProudConfigBuilder {

    OutputSelectionProudConfigBuilder userDefinedPartitioning();

    OutputSelectionProudConfigBuilder replicationPartitioned();

    OutputSelectionProudConfigBuilder gridPartitionedUsing(GridPartitioning.GridPartitioner gridPartitioner);

    default OutputSelectionProudConfigBuilder treePartitionedUsing(String initialNodeFile) {
        return treePartitionedUsing(initialNodeFile, 10000);
    }


    OutputSelectionProudConfigBuilder treePartitionedUsing(String initialNodeFile, int initialNodeCount);

}
