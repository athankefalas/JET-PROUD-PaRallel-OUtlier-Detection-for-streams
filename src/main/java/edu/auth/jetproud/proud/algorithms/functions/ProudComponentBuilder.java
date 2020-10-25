package edu.auth.jetproud.proud.algorithms.functions;

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.ProudContext;
import edu.auth.jetproud.proud.algorithms.OutlierQuery;
import edu.auth.jetproud.proud.state.ProudStatistics;
import edu.auth.jetproud.utils.Tuple;

import java.util.LinkedList;
import java.util.List;

public final class ProudComponentBuilder
{
    private ProudContext proudContext;
    private final ProudStatistics statistics;

    private ProudComponentBuilder(ProudContext proudContext, ProudStatistics statistics) {
        this.proudContext = proudContext;
        this.statistics = statistics;
    }

    public static ProudComponentBuilder create(ProudContext proudContext) {
        return new ProudComponentBuilder(proudContext, ProudStatistics.global());
    }

    public <T extends AnyProudData> ProudAccumulateFunction<T> accumulator(ProudAccumulateFunction.AccumulateFunction<T> function) {
        return new ProudAccumulateFunction<>(proudContext, statistics, function);
    }

    public <T extends AnyProudData> AggregateOperation1<KeyedWindowResult<Integer, List<Tuple<Integer, T>>>, List<T>, List<T>> outlierAggregator(ProudAccumulateFunction.AccumulateFunction<T> accumulateFunction) {
        return AggregateOperation.withCreate(()->(List<T>)new LinkedList<T>())
                .andAccumulate(accumulator(accumulateFunction))
                .andExportFinish((it)->it);
    }

    public <T extends AnyProudData> AggregateOperation1<Tuple<Integer,T>,List<Tuple<Integer, T>>,List<Tuple<Integer, T>>> windowAggregator() {
        return AggregateOperation.withCreate(()-> (List<Tuple<Integer, T>>) new LinkedList<Tuple<Integer, T>>())
                .<Tuple<Integer,T>>andAccumulate(List::add)
                .andCombine((acc, other)-> {
                    for (Tuple<Integer, T> item:other) {
                        if (acc.contains(item))
                            continue;

                        if (acc.stream().anyMatch((it)-> it.first.equals(item.first) && it.second.id == item.second.id)) {
                            continue;
                        }

                        acc.add(item);
                    }
                })
                .andExportFinish((acc)-> acc);
    }

    /// Metadata Window

    public <T extends AnyProudData> AggregateOperation1<T, List<T>, List<T>> metaWindowAggregator() {
        return AggregateOperation.withCreate(()-> (List<T>) new LinkedList<T>())
                .<T>andAccumulate(List::add)
                .andCombine((acc, other)-> {
                    for (T item:other) {
                        if (acc.contains(item))
                            continue;

                        if (acc.stream().anyMatch((it)-> it.id == item.id)) {
                            continue;
                        }

                        acc.add(item);
                    }
                })
                .andExportFinish((acc)-> acc);
    }


    public <T extends AnyProudData> AggregateOperation1<KeyedWindowResult<Integer, List<T>>, List<Tuple<Long,OutlierQuery>>, List<Tuple<Long,OutlierQuery>>> metadataAggregator(BiConsumerEx<List<Tuple<Long,OutlierQuery>>, KeyedWindowResult<Integer, List<T>>> accumulateFunction) {
        return AggregateOperation.withCreate(()->(List<Tuple<Long,OutlierQuery>>)new LinkedList<Tuple<Long,OutlierQuery>>())
                .andAccumulate(accumulateFunction)
                .andExportFinish((it)->it);
    }



}
