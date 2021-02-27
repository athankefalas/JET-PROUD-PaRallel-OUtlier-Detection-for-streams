package edu.auth.jetproud.proud.sink.logger;

import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.sink.AnyProudSink;

public class ProudPrintSink extends AnyProudSink<String>
{
    public ProudPrintSink(ProudContext proudContext) {
        super(proudContext);
    }

    @Override
    public Sink<String> createJetSink() {
        return Sinks.logger((it)->it);
    }

    @Override
    public String convertResultItem(long key, OutlierQuery query) {
        return key +
                ";(" +
                query.window + "," +
                query.slide + "," +
                query.range + "," +
                query.kNeighbours + ")" +
                ";" +
                query.outlierCount;
    }
}
