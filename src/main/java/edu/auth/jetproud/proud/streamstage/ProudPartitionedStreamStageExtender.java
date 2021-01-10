package edu.auth.jetproud.proud.streamstage;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.exceptions.ProudException;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.algorithms.ProudKeyedWindow;
import edu.auth.jetproud.proud.algorithms.executors.custom.CustomFunctionProudAlgorithmExecutor;
import edu.auth.jetproud.proud.algorithms.executors.custom.UserDefinedProudAlgorithmExecutor;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.algorithms.contracts.ProudAlgorithmExecutor;
import edu.auth.jetproud.proud.extension.AnyProudJetClassExtender;
import edu.auth.jetproud.proud.extension.proxy.ProxyExtension;
import edu.auth.jetproud.proud.partitioning.PartitionedData;
import edu.auth.jetproud.utils.Tuple;

import java.io.Serializable;
import java.util.Map;

public class ProudPartitionedStreamStageExtender<T extends AnyProudData> extends AnyProudJetClassExtender<StreamStage<PartitionedData<T>>> implements ProudPartitionedStreamStage.Implementor<T>
{

    public ProudPartitionedStreamStageExtender(ProudContext proudContext) {
        super(proudContext);
    }

    @Override
    public ProudOutliersDetectedStreamStage detectOutliers()  throws ProudException {
        ProudAlgorithmExecutor executor = ProudAlgorithmExecutor.create(proudContext);
        StreamStage<Tuple<Long, OutlierQuery>> jetStreamStage = executor.execute(target);

        return ProxyExtension.of(jetStreamStage, ProudOutliersDetectedStreamStage.class,
                new ProudOutliersDetectedStreamStageExtender(proudContext)
        );
    }

    @Override
    public <D extends AnyProudData> ProudOutliersDetectedStreamStage detectOutliers(UserDefinedProudAlgorithmExecutor<D> executor) throws ProudException {
        StreamStage<Tuple<Long, OutlierQuery>> jetStreamStage = executor.execute(target);

        return ProxyExtension.of(jetStreamStage, ProudOutliersDetectedStreamStage.class,
                new ProudOutliersDetectedStreamStageExtender(proudContext)
        );
    }

    @Override
    public <D extends AnyProudData, S extends Serializable> ProudOutliersDetectedStreamStage detectOutliers(FunctionEx<AnyProudData, D> converter, BiFunctionEx<ProudKeyedWindow<D>, Map<String, S>, OutlierQuery> outlierDetectionFunction) throws ProudException {
        CustomFunctionProudAlgorithmExecutor<D,S> executor = new CustomFunctionProudAlgorithmExecutor<>(proudContext, converter, outlierDetectionFunction);
        StreamStage<Tuple<Long, OutlierQuery>> jetStreamStage = executor.execute(target);

        return ProxyExtension.of(jetStreamStage, ProudOutliersDetectedStreamStage.class,
                new ProudOutliersDetectedStreamStageExtender(proudContext)
        );
    }
}
