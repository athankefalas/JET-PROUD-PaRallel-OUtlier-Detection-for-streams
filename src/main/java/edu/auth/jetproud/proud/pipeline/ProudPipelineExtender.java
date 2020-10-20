package edu.auth.jetproud.proud.pipeline;

import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.ProudContext;
import edu.auth.jetproud.proud.extension.AnyProudJetClassExtender;
import edu.auth.jetproud.proud.extension.proxy.ProxyExtension;
import edu.auth.jetproud.proud.source.ProudSource;
import edu.auth.jetproud.proud.streamstage.ProudStreamStage;
import edu.auth.jetproud.proud.streamstage.ProudStreamStageExtender;

public class ProudPipelineExtender extends AnyProudJetClassExtender<Pipeline> implements ProudPipeline.ExtensionImplementor
{

    public ProudPipelineExtender(ProudContext proudContext) {
        super(proudContext);
    }


    @Override
    public ProudStreamStage<AnyProudData> readFrom(ProudSource<AnyProudData> source) {
        long allowedLag = proudContext.getProudInternalConfiguration().getAllowedLateness();
        StreamStage<AnyProudData> jetStreamStage = source.readInto(target)
                .withNativeTimestamps(allowedLag);

        return (ProudStreamStage<AnyProudData>) ProxyExtension.of(jetStreamStage, ProudStreamStage.class, new ProudStreamStageExtender<AnyProudData>(proudContext));
    }
}
