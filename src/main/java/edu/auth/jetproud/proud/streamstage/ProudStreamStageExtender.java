package edu.auth.jetproud.proud.streamstage;

import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.application.parameters.data.ProudPartitioningOption;
import edu.auth.jetproud.application.parameters.errors.ProudArgumentException;
import edu.auth.jetproud.exceptions.ProudException;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.extension.AnyProudJetClassExtender;
import edu.auth.jetproud.proud.extension.proxy.ProxyExtension;
import edu.auth.jetproud.proud.partitioning.*;
import edu.auth.jetproud.proud.partitioning.gridresolvers.DefaultGridPartitioners;
import edu.auth.jetproud.utils.ExceptionUtils;

import java.nio.file.Path;
import java.nio.file.Paths;

public class ProudStreamStageExtender<T extends AnyProudData> extends AnyProudJetClassExtender<StreamStage<T>> implements ProudStreamStage.Implementor<T> {

    public ProudStreamStageExtender(ProudContext proudContext) {
        super(proudContext);
    }

    // Partitioning

    private ProudPartitioning createProudPartitioning() throws ProudException {
        ProudPartitioningOption partitioningOption = proudContext.configuration().getPartitioning();

        int partitionCount = proudContext.internalConfiguration().getPartitions();
        double commonR = proudContext.internalConfiguration().getCommonR();

        // Define here because of java's switch-case scoping
        String dataset;

        switch (partitioningOption) {
            case Replication:
                return new ReplicationPartitioning(proudContext, partitionCount);
            case Grid:
                dataset = proudContext.configuration().getDataset();

                if (dataset == null)
                    throw ExceptionUtils.sneaky(
                            ProudArgumentException.missing("Dataset name","proud configuration")
                    );

                GridPartitioning.GridPartitioner defaultPartitioner = DefaultGridPartitioners.forDatasetNamed(dataset);
                GridPartitioning.GridPartitioner userDefinedPartitioner = proudContext.configuration().getCustomGridPartitioner();

                if (defaultPartitioner == null && userDefinedPartitioner == null)
                    throw ExceptionUtils.sneaky(
                            ProudArgumentException.missing("Grid Partitioner","configuration")
                    );

                GridPartitioning.GridPartitioner gridPartitioner
                        = userDefinedPartitioner == null ? defaultPartitioner : userDefinedPartitioner;

                return new GridPartitioning(proudContext, partitionCount, commonR, gridPartitioner);
            case Tree:
                final int treeInitCount = proudContext.configuration().getTreeInitialNodeCount();
                dataset = proudContext.configuration().getDataset();
                final String datasetHome = proudContext.datasetConfiguration().getDatasetHome();
                final String treeInputFileName = proudContext.treeInitFileName();

                if (datasetHome == null)
                    throw ExceptionUtils.sneaky(
                            ProudArgumentException.missing("Dataset Home Directory","dataset configuration")
                    );

                if (dataset == null)
                    throw ExceptionUtils.sneaky(
                            ProudArgumentException.missing("Dataset name","proud configuration")
                    );

                Path path = Paths.get(datasetHome, dataset, treeInputFileName);
                String initFilePath = path.toString();

                return new TreePartitioning(proudContext, treeInitCount, partitionCount, commonR, initFilePath);
        }

        return new ReplicationPartitioning(proudContext, partitionCount);
    }

    @Override
    public ProudPartitionedStreamStage<AnyProudData> partition() throws Exception {
        final ProudPartitioning proudPartitioning = createProudPartitioning();
        long allowedLag = proudContext.internalConfiguration().getAllowedLateness();

        StreamStage<PartitionedData<AnyProudData>> jetStreamStage = target.flatMap(proudPartitioning::jetPartition)
            .addTimestamps((it)->it.getData().arrival, allowedLag);
        return (ProudPartitionedStreamStage<AnyProudData>) ProxyExtension.of(jetStreamStage,
                ProudPartitionedStreamStage.class,
                new ProudPartitionedStreamStageExtender<AnyProudData>(proudContext)
        );
    }
}
