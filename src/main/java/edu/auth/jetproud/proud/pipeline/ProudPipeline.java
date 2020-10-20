package edu.auth.jetproud.proud.pipeline;

import com.hazelcast.jet.pipeline.*;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.ProudContext;
import edu.auth.jetproud.proud.extension.ClassExtension;
import edu.auth.jetproud.proud.extension.proxy.ProxyExtension;
import edu.auth.jetproud.proud.source.ProudSource;
import edu.auth.jetproud.proud.streamstage.ProudStreamStage;

public interface ProudPipeline extends Pipeline
{

    @ClassExtension(ProudPipelineExtender.class)
    ProudStreamStage<AnyProudData> readFrom(ProudSource<AnyProudData> source);

    //<T> StreamSourceStage<T> readFrom(@Nonnull StreamSource<? extends T> source);

    // <T> StreamSourceStage<T> partitioned();

    // <T> StreamSourceStage<T>

    default StreamStage<AnyProudData> _postSource(StreamSourceStage<AnyProudData> it) {
        it.withIngestionTimestamps();
        it.withNativeTimestamps(0);
        it.withoutTimestamps();
        return it.withTimestamps(null, 0);
    }

    default StageWithWindow<AnyProudData> _postSomething(StreamStage<AnyProudData> it) {
        it.window(WindowDefinition.sliding(0,0)).streamStage();
        return null;
    }

    default StreamStage<AnyProudData> _postSourceStage(StreamStage<AnyProudData> it) {
        return it.map((obj)->null);
    }

    interface ExtensionImplementor {

        ProudStreamStage<AnyProudData> readFrom(ProudSource<AnyProudData> source);

    }


    // ProudJetPipeline Factory

    static ProudPipeline create(ProudContext proudContext) {
        return ProxyExtension.of(Pipeline.create(),
                ProudPipeline.class,
                new ProudPipelineExtender(proudContext)
        );
    }

}

