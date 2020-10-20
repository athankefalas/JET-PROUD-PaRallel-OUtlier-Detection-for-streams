package edu.auth.jetproud.proud.streamstage;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.application.parameters.data.ProudPartitioningOption;
import edu.auth.jetproud.exceptions.ProudException;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.ProudContext;
import edu.auth.jetproud.proud.extension.AnyProudJetClassExtender;
import edu.auth.jetproud.proud.extension.ClassExtender;
import edu.auth.jetproud.proud.extension.proxy.ProxyExtension;
import edu.auth.jetproud.proud.partitioning.*;

public class ProudStreamStageExtender<T extends AnyProudData> extends AnyProudJetClassExtender<StreamStage<T>> implements ProudStreamStage.Implementor<T> {

    public ProudStreamStageExtender(ProudContext proudContext) {
        super(proudContext);
    }

    // Partitioning

    private ProudPartitioning createProudPartitioning() throws ProudException {
        ProudPartitioningOption partitioningOption = proudContext.getProudConfiguration().getPartitioning();

        int partitionCount = proudContext.getProudInternalConfiguration().getPartitions();
        double commonR = proudContext.getProudInternalConfiguration().getCommonR();
        int treeInitCount = proudContext.getProudConfiguration().getTreeInitialNodeCount();

        String initFilePath = "";

        switch (partitioningOption) {
            case Replication:
                return new ReplicationPartitioning(partitionCount);
            case Grid:
                return new GridPartitioning(partitionCount, commonR, null);
            case Tree:
                return new TreePartitioning(treeInitCount, partitionCount, commonR, initFilePath);
        }

        return new ReplicationPartitioning(partitionCount);
    }

    @Override
    public ProudPartitionedStreamStage<AnyProudData> partitioned() throws Exception {
        final ProudPartitioning proudPartitioning = createProudPartitioning();
        long allowedLag = proudContext.getProudInternalConfiguration().getAllowedLateness();

        StreamStage<PartitionedData<AnyProudData>> jetStreamStage = target.flatMap(proudPartitioning::jetPartition)
            .addTimestamps((it)->it.getData().arrival, allowedLag);
        return (ProudPartitionedStreamStage<AnyProudData>) ProxyExtension.of(jetStreamStage,
                ProudPartitionedStreamStage.class,
                new ProudPartitionedStreamStageExtender<AnyProudData>(proudContext)
        );
    }
}
