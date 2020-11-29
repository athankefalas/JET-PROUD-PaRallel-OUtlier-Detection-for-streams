package edu.auth.jetproud.proud.algorithms.functions;

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public final class ProudComponentBuilder
{
    private ProudContext proudContext;

    private ProudComponentBuilder(ProudContext proudContext) {
        this.proudContext = proudContext;
    }

    public static ProudComponentBuilder create(ProudContext proudContext) {
        return new ProudComponentBuilder(proudContext);
    }

    public <T extends AnyProudData> ProudAccumulateFunction<T> accumulator(ProudAccumulateFunction.AccumulateFunction<T> function) {
        return new ProudAccumulateFunction<>(proudContext, function);
    }

    public <T extends AnyProudData> ProudOutlierDetectionFunction<T> outlierDetector(ProudOutlierDetectionFunction.AccumulateFunction<T> function) {
        return new ProudOutlierDetectionFunction<>(proudContext, function);
    }

    public <T extends AnyProudData> AggregateOperation1<KeyedWindowResult<Integer, List<Tuple<Integer, T>>>, AppendableTraverser<T>, AppendableTraverser<T>> outlierAggregation(ProudAccumulateFunction.AccumulateFunction<T> accumulateFunction) {
        return AggregateOperation.withCreate(()->new AppendableTraverser<T>(10000))
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
        return AggregateOperation.withCreate(()-> (List<T>)new LinkedList<T>())
                .<T>andAccumulate((ls, it)->{
                    ls.add(it);
                })
                .andCombine((acc, other)-> {
                    acc.addAll(other);
//                    T next = other.next();
//
//                    while (next != null) {
//                        acc.append(next);
//                        next = other.next();
//                    }
                })
                .andExportFinish((acc)-> acc);
    }


    public <T extends AnyProudData> AggregateOperation1<KeyedWindowResult<Integer, List<T>>, AppendableTraverser<Tuple<Long,OutlierQuery>>, AppendableTraverser<Tuple<Long,OutlierQuery>>> metadataAggregation(Class<T> typeHint, BiConsumerEx<AppendableTraverser<Tuple<Long,OutlierQuery>>, KeyedWindowResult<Integer, List<T>>> accumulateFunction) {
        return AggregateOperation.withCreate(()->new AppendableTraverser<Tuple<Long,OutlierQuery>>(10000))
                .andAccumulate(accumulateFunction)
                .andExportFinish((it)->it);
    }

    // Outlier Detection AND Metadata Aggregation

    public <T extends AnyProudData> AggregateOperation1<KeyedWindowResult<Integer, List<Tuple<Integer, T>>>, AppendableTraverser<Tuple<Long,OutlierQuery>>, AppendableTraverser<Tuple<Long,OutlierQuery>>> outlierDetection(ProudOutlierDetectionFunction.AccumulateFunction<T> accumulateFunction) {
        return AggregateOperation.withCreate(()->new AppendableTraverser<Tuple<Long,OutlierQuery>>(10000))
                .andAccumulate(outlierDetector(accumulateFunction))
                .andExportFinish((it)->it);
    }


}
