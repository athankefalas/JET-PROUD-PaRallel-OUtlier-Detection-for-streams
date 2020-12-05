package edu.auth.jetproud.proud.algorithms.functions;

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.utils.Tuple;

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

    public <T extends AnyProudData> AggregateOperation1<Tuple<Integer,T>,List<Tuple<Integer, T>>,List<Tuple<Integer, T>>> windowAggregator() {
        return AggregateOperation.withCreate(()-> (List<Tuple<Integer, T>>) new LinkedList<Tuple<Integer, T>>())
                .<Tuple<Integer,T>>andAccumulate((acc, it) -> acc.add(it))
                .andCombine(List::addAll)
                .andExportFinish((acc)-> acc);
    }

    /// Metadata Window

    public <T extends AnyProudData> AggregateOperation1<T, List<T>, List<T>> metaWindowAggregator() {
        return AggregateOperation.withCreate(()-> (List<T>)new LinkedList<T>())
                .<T>andAccumulate((acc, it) -> acc.add(it))
                .andCombine(List::addAll)
                .andExportFinish((acc)-> acc);
    }


}
