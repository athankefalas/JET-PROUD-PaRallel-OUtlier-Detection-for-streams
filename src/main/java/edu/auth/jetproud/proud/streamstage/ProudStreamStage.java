package edu.auth.jetproud.proud.streamstage;

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.exceptions.ProudException;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.extension.ClassExtension;
import edu.auth.jetproud.proud.partitioning.PartitionedData;
import edu.auth.jetproud.proud.partitioning.custom.PartitioningState;
import edu.auth.jetproud.proud.partitioning.custom.UserDefinedProudPartitioning;

public interface ProudStreamStage<T> extends StreamStage<T>
{

    @ClassExtension(ProudStreamStageExtender.class)
    ProudPartitionedStreamStage<AnyProudData> partition() throws Exception;

    @ClassExtension(ProudStreamStageExtender.class)
    ProudPartitionedStreamStage<AnyProudData> partition(UserDefinedProudPartitioning proudPartitioning) throws Exception;

    @ClassExtension(ProudStreamStageExtender.class)
    ProudPartitionedStreamStage<AnyProudData> partition(BiConsumerEx<AnyProudData, PartitioningState> partitionFunction) throws Exception;


    interface Implementor<T> {

        ProudPartitionedStreamStage<AnyProudData> partition() throws Exception;

        ProudPartitionedStreamStage<AnyProudData> partition(UserDefinedProudPartitioning proudPartitioning) throws Exception;

        ProudPartitionedStreamStage<AnyProudData> partition(BiConsumerEx<AnyProudData, PartitioningState> partitionFunction) throws Exception;

    }

}
