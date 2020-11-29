package edu.auth.jetproud.proud.algorithms;

import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.application.parameters.data.ProudSpaceOption;
import edu.auth.jetproud.exceptions.ProudException;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.algorithms.contracts.ProudAlgorithmExecutor;
import edu.auth.jetproud.proud.algorithms.exceptions.UnsupportedSpaceException;
import edu.auth.jetproud.proud.algorithms.functions.ProudComponentBuilder;
import edu.auth.jetproud.proud.partitioning.PartitionedData;
import edu.auth.jetproud.proud.state.ProudStatistics;
import edu.auth.jetproud.utils.Tuple;

import java.util.List;

public abstract class AnyProudAlgorithmExecutor<T extends AnyProudData> implements ProudAlgorithmExecutor
{
    public static final String SLIDE_COUNTER = "SLIDE_COUNTER";
    public static final String CPU_TIME_COUNTER = "CPU_TIME_COUNTER";

    protected final ProudContext proudContext;
    protected final ProudAlgorithmOption algorithm;

    public AnyProudAlgorithmExecutor(ProudContext proudContext, ProudAlgorithmOption algorithm) {
        this.proudContext = proudContext;
        this.algorithm = algorithm;
    }

    @Override
    public ProudAlgorithmOption algorithm() {
        return algorithm;
    }

    @Override
    public void createDistributableData() {
        ProudStatistics.slideCounter();
        ProudStatistics.cpuTimeCounter();
    }

    // Implementation

    public abstract List<ProudSpaceOption> supportedSpaceOptions();

    protected abstract <D extends AnyProudData> T transform(D point);

    protected <D extends AnyProudData> StreamStage<Tuple<Integer, T>> prepareStage(StreamStage<PartitionedData<D>> streamStage) {
        return streamStage.map((it)-> new Tuple<>(it.getPartition(), transform(it.getData())));
    }

    @Override
    public <D extends AnyProudData> StreamStage<Tuple<Long, OutlierQuery>> execute(StreamStage<PartitionedData<D>> streamStage) throws ProudException {
        ProudComponentBuilder functionBuilder = ProudComponentBuilder.create(proudContext);
        ProudSpaceOption spaceOption = proudContext.configuration().getSpace();

        long windowSize = proudContext.internalConfiguration().getCommonW();
        long windowSlideSize = proudContext.internalConfiguration().getCommonS();

        StreamStage<KeyedWindowResult<Integer, List<Tuple<Integer, T>>>> windowedStage = prepareStage(streamStage)
                .window(WindowDefinition.sliding(windowSize,windowSlideSize))
                .groupingKey(Tuple::getFirst)
                .aggregate(functionBuilder.windowAggregator());

        switch (spaceOption) {
            case Single:
                return processSingleSpace(windowedStage);
            case MultiQueryMultiParams:
                return processMultiQueryParamsSpace(windowedStage);
            case MultiQueryMultiParamsMultiWindowParams:
                return processMultiQueryParamsMultiWindowParamsSpace(windowedStage);
        }

        return null;
    }

    protected StreamStage<Tuple<Long, OutlierQuery>> processSingleSpace(StreamStage<KeyedWindowResult<Integer, List<Tuple<Integer, T>>>> windowedStage) throws UnsupportedSpaceException {
        throw new UnsupportedSpaceException(ProudSpaceOption.Single, algorithm);
    }

    protected StreamStage<Tuple<Long, OutlierQuery>> processMultiQueryParamsSpace(StreamStage<KeyedWindowResult<Integer, List<Tuple<Integer, T>>>> windowedStage) throws UnsupportedSpaceException {
        throw new UnsupportedSpaceException(ProudSpaceOption.MultiQueryMultiParams, algorithm);
    }

    protected StreamStage<Tuple<Long, OutlierQuery>> processMultiQueryParamsMultiWindowParamsSpace(StreamStage<KeyedWindowResult<Integer, List<Tuple<Integer, T>>>> windowedStage) throws UnsupportedSpaceException {
        throw new UnsupportedSpaceException(ProudSpaceOption.MultiQueryMultiParamsMultiWindowParams, algorithm);
    }
}
