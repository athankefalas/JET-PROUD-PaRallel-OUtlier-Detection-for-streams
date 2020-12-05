package edu.auth.jetproud.proud.pipeline;

import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.extension.AnyProudJetClassExtender;
import edu.auth.jetproud.proud.extension.proxy.ProxyExtension;
import edu.auth.jetproud.proud.source.ProudSource;
import edu.auth.jetproud.proud.streamstage.ProudStreamStage;
import edu.auth.jetproud.proud.streamstage.ProudStreamStageExtender;

public class ProudPipelineExtender extends AnyProudJetClassExtender<Pipeline> implements ProudPipeline.Implementor
{

    public ProudPipelineExtender(ProudContext proudContext) {
        super(proudContext);
    }

    @Override
    public Pipeline jetPipeline() {
        return target;
    }

    @Override
    public ProudContext proudContext() {
        return proudContext;
    }

    @Override
    public ProudStreamStage<AnyProudData> readFrom(ProudSource<AnyProudData> source) {
        StreamStage<AnyProudData> jetStreamStage = source.readInto(target);//.peek();

        return (ProudStreamStage<AnyProudData>) ProxyExtension.of(jetStreamStage, ProudStreamStage.class, new ProudStreamStageExtender<AnyProudData>(proudContext));
    }

    @Override
    public ProudStreamStage<AnyProudData> readData() {
        return readFrom(ProudSource.auto(proudContext));
    }
}
