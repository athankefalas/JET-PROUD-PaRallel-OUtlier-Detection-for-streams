package edu.auth.jetproud.proud.streamstage;

import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.partitioning.PartitionedData;

public interface ProudPartitionedStreamStage<T extends AnyProudData> extends StreamStage<PartitionedData<T>>
{


    interface Implementor<T> {

        //ProudStreamStage<PartitionedData<AnyProudData>> partitioned() throws Exception;

    }

}
