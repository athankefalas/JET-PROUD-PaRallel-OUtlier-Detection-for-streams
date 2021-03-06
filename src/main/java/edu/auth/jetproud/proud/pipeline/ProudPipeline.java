package edu.auth.jetproud.proud.pipeline;

import com.hazelcast.jet.pipeline.*;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.contracts.ProudDataConvertible;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.extension.ClassExtension;
import edu.auth.jetproud.proud.extension.proxy.ProxyExtension;
import edu.auth.jetproud.proud.source.ProudSource;
import edu.auth.jetproud.proud.streamstage.ProudStreamStage;
import edu.auth.jetproud.proud.streamstage.ProudStreamStageExtender;

public interface ProudPipeline extends Pipeline
{

    @ClassExtension(ProudPipelineExtender.class)
    Pipeline jetPipeline();

    @ClassExtension(ProudPipelineExtender.class)
    ProudContext proudContext();

    @ClassExtension(ProudPipelineExtender.class)
    ProudStreamStage<AnyProudData> readFrom(ProudSource<AnyProudData> source);

    @ClassExtension(ProudPipelineExtender.class)
    ProudStreamStage<AnyProudData> readData();

    interface Implementor {

        Pipeline jetPipeline();

        ProudContext proudContext();

        ProudStreamStage<AnyProudData> readFrom(ProudSource<AnyProudData> source);

        ProudStreamStage<AnyProudData> readData();
    }

    // ProudJetPipeline Factory

    static <T extends ProudDataConvertible> ProudStreamStage<AnyProudData> from(StreamStage<T> streamStage, ProudContext proudContext) {
        StreamStage<AnyProudData> convertedStage = streamStage.map(ProudDataConvertible::toProudData);

        return (ProudStreamStage<AnyProudData>) ProxyExtension.of(convertedStage, ProudStreamStage.class, new ProudStreamStageExtender<AnyProudData>(proudContext));
    }

    static ProudPipeline create(ProudContext proudContext) {
        return ProxyExtension.of(Pipeline.create(),
                ProudPipeline.class,
                new ProudPipelineExtender(proudContext)
        );
    }

}

