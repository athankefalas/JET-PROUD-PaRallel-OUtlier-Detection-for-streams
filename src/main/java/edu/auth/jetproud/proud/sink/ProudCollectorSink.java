package edu.auth.jetproud.proud.sink;

import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SinkBuilder;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.sink.collectors.StreamOutliersCollector;
import edu.auth.jetproud.utils.Tuple;

import java.io.Serializable;

public class ProudCollectorSink extends AnyProudSink<Tuple<Long, OutlierQuery>>
{
    private final StreamOutliersCollector collector;

    public ProudCollectorSink(ProudContext proudContext, StreamOutliersCollector collector) {
        super(proudContext);
        this.collector = collector;
    }

    @Override
    public Sink<Tuple<Long, OutlierQuery>> createJetSink() {
        return SinkBuilder.sinkBuilder("collector-sink", pctx -> new Context(collector))
                .receiveFn(Context::accept)
                .destroyFn(Context::close)
                .build();
    }

    @Override
    public Tuple<Long, OutlierQuery> convertResultItem(long key, OutlierQuery query) {
        return new Tuple<>(key, query);
    }

    public static class Context implements StreamOutliersCollector, Serializable
    {
        private StreamOutliersCollector collector;

        public Context(StreamOutliersCollector collector) {
            this.collector = collector;
        }

        @Override
        public void collect(long windowKey, OutlierQuery query, long outlierCount) {
            collector.collect(windowKey, query, outlierCount);
        }

        public void accept(Tuple<Long, OutlierQuery> item) {
            long windowKey = item.first;
            long outlierCount = item.second.outlierCount;
            OutlierQuery query = item.second.withOutlierCount(0);

            collect(windowKey, query, outlierCount);
        }

        public void close() {
            collector.close();
        }
    }

}
