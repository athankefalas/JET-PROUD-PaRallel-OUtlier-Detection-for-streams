package edu.auth.jetproud.proud.streamstage;

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.application.parameters.data.ProudPartitioningOption;
import edu.auth.jetproud.application.parameters.errors.ProudArgumentException;
import edu.auth.jetproud.exceptions.ProudException;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.extension.AnyProudJetClassExtender;
import edu.auth.jetproud.proud.extension.proxy.ProxyExtension;
import edu.auth.jetproud.proud.partitioning.*;
import edu.auth.jetproud.proud.partitioning.custom.CustomFunctionProudPartitioning;
import edu.auth.jetproud.proud.partitioning.custom.PartitioningState;
import edu.auth.jetproud.proud.partitioning.custom.UserDefinedProudPartitioning;
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

        // Define here because of java's switch-case scoping issues
        String dataset;

        switch (partitioningOption) {
            case UserDefined:
                throw ExceptionUtils.sneaky(
                        ProudArgumentException.invalid("Auto partitioning is not compatible with a User Defined partitioning method.")
                );
            case Replication:
                return new ReplicationPartitioning(proudContext);
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

                return new GridPartitioning(proudContext, gridPartitioner);
            case Tree:
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

                Path path = Paths.get(datasetHome, treeInputFileName);
                String initFilePath = path.toString();

                return new TreePartitioning(proudContext, initFilePath);
        }

        throw ExceptionUtils.never();
    }

    @Override
    public ProudPartitionedStreamStage<AnyProudData> partition() throws Exception {
        final ProudPartitioning proudPartitioning = createProudPartitioning();

        StreamStage<PartitionedData<AnyProudData>> jetStreamStage = target.flatMap(proudPartitioning::jetPartition);
        return (ProudPartitionedStreamStage<AnyProudData>) ProxyExtension.of(jetStreamStage,
                ProudPartitionedStreamStage.class,
                new ProudPartitionedStreamStageExtender<AnyProudData>(proudContext)
        );
    }

    @Override
    public ProudPartitionedStreamStage<AnyProudData> partition(UserDefinedProudPartitioning proudPartitioning) throws Exception {
        StreamStage<PartitionedData<AnyProudData>> jetStreamStage = target.flatMap(proudPartitioning::jetPartition);
        return (ProudPartitionedStreamStage<AnyProudData>) ProxyExtension.of(jetStreamStage,
                ProudPartitionedStreamStage.class,
                new ProudPartitionedStreamStageExtender<AnyProudData>(proudContext)
        );
    }

    @Override
    public ProudPartitionedStreamStage<AnyProudData> partition(BiConsumerEx<AnyProudData, PartitioningState> partitionFunction) throws Exception {
        ProudPartitioning proudPartitioning = new CustomFunctionProudPartitioning(proudContext, partitionFunction);

        StreamStage<PartitionedData<AnyProudData>> jetStreamStage = target.flatMap(proudPartitioning::jetPartition);
        return (ProudPartitionedStreamStage<AnyProudData>) ProxyExtension.of(jetStreamStage,
                ProudPartitionedStreamStage.class,
                new ProudPartitionedStreamStageExtender<AnyProudData>(proudContext)
        );
    }
}
