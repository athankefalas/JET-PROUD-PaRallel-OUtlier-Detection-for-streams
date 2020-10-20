package edu.auth.jetproud.proud.source;

import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamSourceStage;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.ProudContext;

public interface ProudSource<T extends AnyProudData>
{

    StreamSource<T> createJetSource();

    default StreamSourceStage<T> readInto(Pipeline pipeline) {
        return pipeline.readFrom(createJetSource());
    }

    static ProudSource<AnyProudData> debugFileSource(ProudContext context) {
        return new ProudFileSource<>(context, ProudFileSource.proudDataParser("&",";"));
    }

    static ProudSource<AnyProudData> debugFileSource(ProudContext context, String fieldDelimiter, String valueDelimiter) {
        return new ProudFileSource<>(context, ProudFileSource.proudDataParser(fieldDelimiter,valueDelimiter));
    }

    static ProudSource<AnyProudData> kafkaSource(ProudContext context) {
        return new ProudKafkaSource<>(context, null);
    }

}
