package edu.auth.jetproud.proud.streamstage;

import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.exceptions.ProudException;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.ProudContext;
import edu.auth.jetproud.proud.algorithms.contracts.ProudAlgorithmExecutor;
import edu.auth.jetproud.proud.extension.AnyProudJetClassExtender;
import edu.auth.jetproud.proud.partitioning.PartitionedData;

public class ProudPartitionedStreamStageExtender<T extends AnyProudData> extends AnyProudJetClassExtender<StreamStage<PartitionedData<T>>> implements ProudPartitionedStreamStage.Implementor<T>
{


    public ProudPartitionedStreamStageExtender(ProudContext proudContext) {
        super(proudContext);
    }



    void todo() throws ProudException {
        ProudAlgorithmExecutor executor = ProudAlgorithmExecutor.create(proudContext);
        Object anyStage = executor.execute(target);
        //return anyStage;
    }


}
