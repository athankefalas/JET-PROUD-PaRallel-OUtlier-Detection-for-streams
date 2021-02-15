package edu.auth.jetproud.proud.streamstage;

import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.pipeline.SinkStage;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.extension.AnyProudJetClassExtender;
import edu.auth.jetproud.proud.sink.ProudSink;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.util.List;

public class ProudOutliersDetectedStreamStageExtender extends AnyProudJetClassExtender<StreamStage<Tuple<Long, OutlierQuery>>> implements ProudOutliersDetectedStreamStage.Implementor
{
    public ProudOutliersDetectedStreamStageExtender(ProudContext proudContext) {
        super(proudContext);
    }

    @Override
    public <T> SinkStage aggregateAndWriteTo(ProudSink<T> proudSink) {
        return proudSink.write(target);
    }

    @Override
    public StreamStage<Tuple<Long, OutlierQuery>> aggregate() {
        long slideSize = proudContext.internalConfiguration().getCommonS();

        return target
                .groupingKey(Tuple::getKey)
                .window(WindowDefinition.tumbling(slideSize))
                .aggregate(
                        AggregateOperation.withCreate(()->new AppendableTraverser<Tuple<Long, OutlierQuery>>(100000))
                                .<Tuple<Long, OutlierQuery>>andAccumulate((acc, it)-> acc.append(it))
                                .andCombine((acc, other)->{
                                    Tuple<Long,OutlierQuery> item = other.next();
                                    while(item != null) {
                                        acc.append(item);
                                        item = other.next();
                                    }
                                })
                                .andExportFinish((acc)->acc)
                )
                .flatMap((window)->{
                    List<Tuple<Long, OutlierQuery>> elements = Lists.make();

                    Tuple<Long, OutlierQuery> item = window.getValue().next();

                    while (item != null) {
                        elements.add(item);
                        item = window.getValue().next();
                    }

                    OutlierQuery coalesced = elements.stream()
                            .map(Tuple::getSecond)
                            .reduce(new OutlierQuery(0,0,0,0), (res, value)->{
                                long count = res.outlierCount + value.outlierCount;
                                return value.withOutlierCount(count);
                            });

                    Tuple<Long, OutlierQuery> result = new Tuple<>(window.key(), coalesced);
                    return Traversers.singleton(result);
                });
    }

    @Override
    public SinkStage sinkData() {
        ProudSink<?> sink = ProudSink.auto(proudContext);
        return aggregateAndWriteTo(sink);
    }
}
