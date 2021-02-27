package edu.auth.jetproud.proud.sink;

import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkStage;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.sink.collectors.ProudCollectorSink;
import edu.auth.jetproud.proud.sink.collectors.StreamOutliersCollector;
import edu.auth.jetproud.proud.sink.influxdb.ProudInfluxDBSink;
import edu.auth.jetproud.proud.sink.logger.ProudPrintSink;
import edu.auth.jetproud.utils.Tuple;
import org.influxdb.dto.Point;

import java.io.Serializable;

public interface ProudSink<T> extends Serializable
{

    Sink<T> createJetSink();

    StreamStage<T> convertStage(StreamStage<Tuple<Long, OutlierQuery>> streamStage);

    default SinkStage write(StreamStage<Tuple<Long, OutlierQuery>> detectedStreamStage) {
        StreamStage<T> writableStage = convertStage(detectedStreamStage);
        return writableStage.writeTo(createJetSink());
    }

    //// Factory Methods

    static ProudSink<?> auto(ProudContext context) {
        // Select the data source automatically based on config
        switch (context.outputType()) {
            case Print:
                return logger(context);
            case InfluxDB:
                return influxDB(context);
            default:
                if (context.configuration().getDebug()) {
                    return logger(context);
                } else {
                    return influxDB(context);
                }
        }
    }

    static ProudSink<String> logger(ProudContext proudContext) {
        return new ProudPrintSink(proudContext);
    }

    static ProudSink<Point> influxDB(ProudContext proudContext) {
        return new ProudInfluxDBSink(proudContext);
    }

    static ProudSink<Tuple<Long, OutlierQuery>> collector(ProudContext proudContext, StreamOutliersCollector collector) {
        return new ProudCollectorSink(proudContext, collector);
    }
}
