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

        StreamStage<T> aggregated = streamStage.groupingKey(Tuple::getKey)
                .window(WindowDefinition.tumbling(windowSize))
                .aggregate(
                        AggregateOperation.withCreate(()->new LinkedList<Tuple<Long, OutlierQuery>>())
                        .<Tuple<Long, OutlierQuery>>andAccumulate(LinkedList::add)
                                .andCombine((acc, other)->{

                                    for (Tuple<Long,OutlierQuery> item:other) {
                                        List<Tuple<Long,OutlierQuery>> matches = acc.stream()
                                                .filter((it)-> it.first.equals(item.first))
                                                .filter((it)->it.second.isInSameSpace(it.second))
                                                .collect(Collectors.toList());

                                        if (matches.isEmpty()) {
                                            acc.add(item);
                                            continue;
                                        }

                                        acc.removeAll(matches);

                                        matches.add(item);

                                        Tuple<Long,OutlierQuery> coalesced = matches.stream()
                                                .reduce(new Tuple<>(item.first, item.second.withOutlierCount(0)), (a, b)->{
                                                    long window = a.first;
                                                    long count = a.second.outlierCount + b.second.outlierCount;

                                                    return new Tuple<>(window, a.second.withOutlierCount(count));
                                                });

                                        acc.add(coalesced);
                                    }
                                })
                        .andExportFinish((acc)->acc)
                )
                .rollingAggregate(
                        AggregateOperation.withCreate(()->new AppendableTraverser<T>(1000))
                                .<KeyedWindowResult<Long, LinkedList<Tuple<Long, OutlierQuery>>>>andAccumulate((acc, window)->{
                                    List<Tuple<Long, OutlierQuery>> elements = window.getValue();

                                    OutlierQuery coalesced = elements.stream()
                                            .map(Tuple::getSecond)
                                            .reduce(new OutlierQuery(0,0,0,0), (res, value)->{
                                                long count = res.outlierCount + value.outlierCount;
                                                return value.withOutlierCount(count);
                                            });

                                    T convertedResult = convertResultItem(window.key(), coalesced);
                                    acc.append(convertedResult);
                                })
                                .andExportFinish((it)->it)
                )
                .flatMap((it)->it);

        return aggregated;
    }


}
