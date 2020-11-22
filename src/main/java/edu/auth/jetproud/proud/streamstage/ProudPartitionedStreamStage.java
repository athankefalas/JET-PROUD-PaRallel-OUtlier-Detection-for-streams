package edu.auth.jetproud.proud.streamstage;

import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.exceptions.ProudException;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.extension.ClassExtension;
import edu.auth.jetproud.proud.partitioning.PartitionedData;
import edu.auth.jetproud.utils.Tuple;

public interface ProudPartitionedStreamStage<T extends AnyProudData> extends StreamStage<PartitionedData<T>>
{
    @ClassExtension(ProudPartitionedStreamStageExtender.class)
    ProudOutliersDetectedStreamStage detectOutliers() throws ProudException;

    interface Implementor<T> {

        ProudOutliersDetectedStreamStage detectOutliers() throws ProudException;

    }

}
