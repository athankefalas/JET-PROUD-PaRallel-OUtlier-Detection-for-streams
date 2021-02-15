package edu.auth.jetproud.proud.streamstage;

import com.hazelcast.jet.pipeline.SinkStage;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.extension.ClassExtension;
import edu.auth.jetproud.proud.pipeline.ProudPipelineExtender;
import edu.auth.jetproud.proud.sink.ProudSink;
import edu.auth.jetproud.utils.Tuple;

public interface ProudOutliersDetectedStreamStage extends StreamStage<Tuple<Long, OutlierQuery>> {

    @ClassExtension(ProudOutliersDetectedStreamStageExtender.class)
    <T> SinkStage aggregateAndWriteTo(ProudSink<T> proudSink);

    @ClassExtension(ProudOutliersDetectedStreamStageExtender.class)
    StreamStage<Tuple<Long, OutlierQuery>> aggregate();

    @ClassExtension(ProudOutliersDetectedStreamStageExtender.class)
    SinkStage sinkData();

    interface Implementor {
        <T> SinkStage aggregateAndWriteTo(ProudSink<T> proudSink);

        StreamStage<Tuple<Long, OutlierQuery>> aggregate();

        SinkStage sinkData();
    }

}
