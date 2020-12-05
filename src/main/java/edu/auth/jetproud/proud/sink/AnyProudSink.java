package edu.auth.jetproud.proud.sink;

import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AnyProudSink<T> implements ProudSink<T>
{
    protected ProudContext proudContext;

    private AnyProudSink(){}

    public AnyProudSink(ProudContext proudContext) {
        this.proudContext = proudContext;
    }

    public abstract Sink<T> createJetSink();

    public abstract T convertResultItem(long key, OutlierQuery query);

    @Override
    public final StreamStage<T> convertStage(StreamStage<Tuple<Long, OutlierQuery>> streamStage) {
        long windowSize = proudContext.internalConfiguration().getCommonW();

        return streamStage
                .groupingKey(Tuple::getKey)
                .window(WindowDefinition.sliding(windowSize, 5000))
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

                    //System.out.println("EL cnt "+elements.size());
                    OutlierQuery coalesced = elements.stream()
                            .map(Tuple::getSecond)
                            .reduce(new OutlierQuery(0,0,0,0), (res, value)->{
                                long count = res.outlierCount + value.outlierCount;
                                return value.withOutlierCount(count);
                            });

                    T convertedResult = convertResultItem(window.key(), coalesced);
                    return Traversers.singleton(convertedResult);
                });
    }


}
