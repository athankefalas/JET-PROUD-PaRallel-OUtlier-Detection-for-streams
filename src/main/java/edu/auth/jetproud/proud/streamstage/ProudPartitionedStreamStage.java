package edu.auth.jetproud.proud.streamstage;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.exceptions.ProudException;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.algorithms.ProudKeyedWindow;
import edu.auth.jetproud.proud.algorithms.executors.custom.UserDefinedProudAlgorithmExecutor;
import edu.auth.jetproud.proud.extension.ClassExtension;
import edu.auth.jetproud.proud.partitioning.PartitionedData;
import edu.auth.jetproud.utils.Tuple;

import java.io.Serializable;
import java.util.Map;

public interface ProudPartitionedStreamStage<T extends AnyProudData> extends StreamStage<PartitionedData<T>>
{
    @ClassExtension(ProudPartitionedStreamStageExtender.class)
    ProudOutliersDetectedStreamStage detectOutliers() throws ProudException;

    @ClassExtension(ProudPartitionedStreamStageExtender.class)
    <D extends AnyProudData> ProudOutliersDetectedStreamStage detectOutliers(UserDefinedProudAlgorithmExecutor<D> executor) throws ProudException;

    @ClassExtension(ProudPartitionedStreamStageExtender.class)
    <D extends AnyProudData, S extends Serializable> ProudOutliersDetectedStreamStage detectOutliers(FunctionEx<AnyProudData, D> converter, BiFunctionEx<ProudKeyedWindow<D>, Map<String,S>, OutlierQuery> outlierDetectionFunction) throws ProudException;

    interface Implementor<T> {

        ProudOutliersDetectedStreamStage detectOutliers() throws ProudException;

        <D extends AnyProudData> ProudOutliersDetectedStreamStage detectOutliers(UserDefinedProudAlgorithmExecutor<D> executor) throws ProudException;

        <D extends AnyProudData, S extends Serializable> ProudOutliersDetectedStreamStage detectOutliers(FunctionEx<AnyProudData, D> converter, BiFunctionEx<ProudKeyedWindow<D>, Map<String,S>, OutlierQuery> outlierDetectionFunction) throws ProudException;

    }

}
