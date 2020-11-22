package edu.auth.jetproud.proud.streamstage;

import com.hazelcast.jet.pipeline.SinkStage;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.extension.AnyProudJetClassExtender;
import edu.auth.jetproud.proud.sink.ProudSink;
import edu.auth.jetproud.utils.Tuple;

public class ProudOutliersDetectedStreamStageExtender extends AnyProudJetClassExtender<StreamStage<Tuple<Long, OutlierQuery>>> implements ProudOutliersDetectedStreamStage.Implementor
{
    public ProudOutliersDetectedStreamStageExtender(ProudContext proudContext) {
        super(proudContext);
    }

    @Override
    public <T> SinkStage writeTo(ProudSink<T> proudSink) {
        return proudSink.write(target);
    }

    @Override
    public SinkStage sinkData() {
        ProudSink<?> sink = ProudSink.auto(proudContext);
        return writeTo(sink);
    }
}
