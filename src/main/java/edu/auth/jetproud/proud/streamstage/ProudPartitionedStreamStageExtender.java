package edu.auth.jetproud.proud.streamstage;

import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.exceptions.ProudException;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.algorithms.contracts.ProudAlgorithmExecutor;
import edu.auth.jetproud.proud.extension.AnyProudJetClassExtender;
import edu.auth.jetproud.proud.extension.proxy.ProxyExtension;
import edu.auth.jetproud.proud.partitioning.PartitionedData;
import edu.auth.jetproud.utils.Tuple;

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
}
