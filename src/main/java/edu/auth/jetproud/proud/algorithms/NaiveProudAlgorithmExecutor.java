package edu.auth.jetproud.proud.algorithms;

import com.hazelcast.collection.IList;
import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.pipeline.StageWithKeyAndWindow;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.NaiveProudData;
import edu.auth.jetproud.proud.ProudContext;
import edu.auth.jetproud.proud.algorithms.exceptions.UnsupportedSpaceException;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

public class NaiveProudAlgorithmExecutor extends AnyProudAlgorithmExecutor<NaiveProudData>
{

    public NaiveProudAlgorithmExecutor() {
        super(ProudAlgorithmOption.Naive);
    }

    @Override
    protected <D extends AnyProudData> NaiveProudData transform(D point) {
        return new NaiveProudData(point);
    }

    @Override
    protected Object processSingleSpace(StageWithKeyAndWindow<Tuple<Integer, NaiveProudData>, Integer> windowedStage, ProudContext context) throws UnsupportedSpaceException {
        // TODO impl
        StreamStage<KeyedWindowResult<Integer, List<NaiveProudData>>> stage
                = windowedStage.aggregate(aggregator());
        return super.processSingleSpace(windowedStage, context);
    }

    // Aggregator Impl - Find outliers in each window, collect in list and return them
    private AggregateOperation1<Tuple<Integer, NaiveProudData>, List<NaiveProudData>, List<NaiveProudData>> aggregator() {
        BiConsumerEx<List<NaiveProudData>, Tuple<Integer, NaiveProudData>> accumulateFn = (items, point) -> {
            //
        };

        BiConsumerEx<List<NaiveProudData>, List<NaiveProudData>> combineFn = (one, other) -> {

        };

        FunctionEx<List<NaiveProudData>, List<NaiveProudData>> exportFn = (items) -> {
            return items.stream().filter((it)->!it.safe_inlier).collect(Collectors.toList());
        };

        return AggregateOperation.withCreate(()-> (List<NaiveProudData>) new LinkedList<NaiveProudData>())
                .andAccumulate(accumulateFn)
                .andCombine(combineFn)
                .andExportFinish(exportFn);
    }
}
