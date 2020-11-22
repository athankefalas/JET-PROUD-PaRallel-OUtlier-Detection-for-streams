package edu.auth.jetproud.proud.algorithms.functions;

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.distributables.DistributedCounter;
import edu.auth.jetproud.proud.state.ProudStatistics;
import edu.auth.jetproud.utils.Tuple;

import java.io.Serializable;
import java.util.List;
import java.util.function.BiConsumer;

public class ProudAccumulateFunction<T extends AnyProudData> implements BiConsumerEx<List<T>, KeyedWindowResult<Integer, List<Tuple<Integer, T>>>>
{

    public interface AccumulateFunction<T extends AnyProudData> extends BiConsumer<List<T>, KeyedWindowResult<Integer, List<Tuple<Integer, T>>>>, Serializable {

    }

    private ProudContext context;
    private AccumulateFunction<T> accumulateFunction;

    public ProudAccumulateFunction(ProudContext context, AccumulateFunction<T> accumulateFunction) {
        this.context = context;
        this.accumulateFunction = accumulateFunction;
    }

    @Override
    public void acceptEx(List<T> accumulator, KeyedWindowResult<Integer, List<Tuple<Integer, T>>> item) throws Exception {
        // Statistics
        DistributedCounter slideCounter = ProudStatistics.slideCounter();
        DistributedCounter cpuTimeCounter = ProudStatistics.cpuTimeCounter();

        slideCounter.incrementAndGet();
        long startTime = System.currentTimeMillis();

        // Execute Accumulator Function
        accumulateFunction.accept(accumulator, item);

        // Statistics
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        cpuTimeCounter.addAndGet(duration);
    }

}
