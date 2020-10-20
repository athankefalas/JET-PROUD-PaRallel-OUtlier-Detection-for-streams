package edu.auth.jetproud.proud.streamstage;

import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.exceptions.ProudException;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.extension.ClassExtension;
import edu.auth.jetproud.proud.partitioning.PartitionedData;

public interface ProudStreamStage<T> extends StreamStage<T>
{

    @ClassExtension(ProudStreamStageExtender.class)
    ProudPartitionedStreamStage<AnyProudData> partitioned() throws Exception;

    interface Implementor<T> {

        ProudPartitionedStreamStage<AnyProudData> partitioned() throws Exception;

    }

}
