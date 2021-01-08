package edu.auth.jetproud.proud.source;

import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamSourceStage;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.source.streams.StreamGenerator;
import edu.auth.jetproud.proud.source.streams.StreamGenerators;

import java.util.List;

public interface ProudSource<T extends AnyProudData>
{

    StreamStage<T> readInto(Pipeline pipeline);


    //// Factory Methods

    static ProudSource<AnyProudData> auto(ProudContext context) {
        // Select the data source automatically based on config
        switch (context.inputType()) {
            case File:
                return file(context);
            case Kafka:
                return kafkaSource(context);
            default:
                if (context.configuration().getDebug()) {
                    return file(context);
                } else {
                    return kafkaSource(context);
                }
        }
    }

    static ProudSource<AnyProudData> file(ProudContext context) {
        return new ProudFileSource<>(context, ProudFileSource.proudDataParser("&",","));
    }

    static ProudSource<AnyProudData> file(ProudContext context, String fieldDelimiter, String valueDelimiter) {
        return new ProudFileSource<>(context, ProudFileSource.proudDataParser(fieldDelimiter,valueDelimiter));
    }

    static ProudSource<AnyProudData> file(ProudContext context, String fileName) {
        return new ProudFileSource<>(context, fileName, ProudFileSource.proudDataParser("&",","));
    }

    static ProudSource<AnyProudData> file(ProudContext context, String fileName, String fieldDelimiter, String valueDelimiter) {
        return new ProudFileSource<>(context,fileName, ProudFileSource.proudDataParser(fieldDelimiter,valueDelimiter));
    }

    static ProudSource<AnyProudData> streaming(ProudContext context, StreamGenerator streamGenerator) {
        return new ProudStreamSource(context, streamGenerator);
    }

    static ProudSource<AnyProudData> streamingRealTime(ProudContext context, List<AnyProudData> data) {
        return new ProudStreamSource(context, StreamGenerators.realTimeItemsIn(data));
    }

    static ProudSource<AnyProudData> streamingSimulatedTime(ProudContext context, List<AnyProudData> data) {
        return new ProudStreamSource(context, StreamGenerators.simulatedTimeItemsIn(data));
    }

    static ProudSource<AnyProudData> kafkaSource(ProudContext context) {
        return new ProudKafkaSource<>(context, null);
    }

}
