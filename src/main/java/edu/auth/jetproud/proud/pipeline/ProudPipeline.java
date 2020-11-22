package edu.auth.jetproud.proud.pipeline;

import com.hazelcast.jet.pipeline.*;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.extension.ClassExtension;
import edu.auth.jetproud.proud.extension.proxy.ProxyExtension;
import edu.auth.jetproud.proud.source.ProudSource;
import edu.auth.jetproud.proud.streamstage.ProudStreamStage;

public interface ProudPipeline extends Pipeline
{

    @ClassExtension(ProudPipelineExtender.class)
    ProudStreamStage<AnyProudData> readFrom(ProudSource<AnyProudData> source);

    @ClassExtension(ProudPipelineExtender.class)
    ProudStreamStage<AnyProudData> readData();

    interface Implementor {

        ProudStreamStage<AnyProudData> readFrom(ProudSource<AnyProudData> source);

        ProudStreamStage<AnyProudData> readData();
    }

    // ProudJetPipeline Factory

    static ProudPipeline create(ProudContext proudContext) {
        return ProxyExtension.of(Pipeline.create(),
                ProudPipeline.class,
                new ProudPipelineExtender(proudContext)
        );
    }

}

