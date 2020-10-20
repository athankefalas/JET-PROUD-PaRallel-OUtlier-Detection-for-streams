package edu.auth.jetproud.proud.algorithms;

import com.hazelcast.jet.pipeline.StageWithKeyAndWindow;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.application.parameters.data.ProudSpaceOption;
import edu.auth.jetproud.exceptions.ProudException;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.ProudContext;
import edu.auth.jetproud.proud.algorithms.contracts.ProudAlgorithmExecutor;
import edu.auth.jetproud.proud.algorithms.exceptions.UnsupportedSpaceException;
import edu.auth.jetproud.proud.partitioning.PartitionedData;
import edu.auth.jetproud.utils.Tuple;

public abstract class AnyProudAlgorithmExecutor<T extends AnyProudData> implements ProudAlgorithmExecutor
{
    private final ProudAlgorithmOption algorithm;

    public AnyProudAlgorithmExecutor(ProudAlgorithmOption algorithm) {
        this.algorithm = algorithm;
    }

    @Override
    public ProudAlgorithmOption algorithm() {
        return algorithm;
    }

    protected abstract <D extends AnyProudData> T transform(D point);

    protected <D extends AnyProudData> StreamStage<Tuple<Integer, T>> prepareStage(StreamStage<PartitionedData<D>> streamStage) {
        return streamStage.map((it)-> new Tuple<>(it.getPartition(), transform(it.getData())));
    }

    @Override
    public <D extends AnyProudData> Object execute(StreamStage<PartitionedData<D>> streamStage, ProudContext context) throws ProudException {
        ProudSpaceOption spaceOption = context.getProudConfiguration().getSpace();

        long windowSize = context.getProudInternalConfiguration().getCommonW();
        long windowSlideSize = context.getProudInternalConfiguration().getCommonS();

        StageWithKeyAndWindow<Tuple<Integer, T>, Integer> windowedStage = prepareStage(streamStage)
                .groupingKey(Tuple::getFirst)
                .window(WindowDefinition.sliding(windowSize,windowSlideSize));

        switch (spaceOption) {
            case None:
                throw new UnsupportedSpaceException(spaceOption, algorithm);
            case Single:
                return processSingleSpace(windowedStage, context);
            case MultiQueryMultiParams:
                return processMultiQueryParamsSpace(windowedStage, context);
            case MultiQueryMultiParamsMultiWindowParams:
                return processMultiQueryMultiParamsMultiWindowParamsSpace(windowedStage, context);
        }

        return null;
    }

    protected Object processSingleSpace(StageWithKeyAndWindow<Tuple<Integer, T>, Integer> windowedStage, ProudContext context) throws UnsupportedSpaceException {
        throw new UnsupportedSpaceException(ProudSpaceOption.Single, algorithm);
    }

    protected Object processMultiQueryParamsSpace(StageWithKeyAndWindow<Tuple<Integer, T>, Integer> windowedStage, ProudContext context) throws UnsupportedSpaceException {
        throw new UnsupportedSpaceException(ProudSpaceOption.MultiQueryMultiParams, algorithm);
    }

    protected Object processMultiQueryMultiParamsMultiWindowParamsSpace(StageWithKeyAndWindow<Tuple<Integer, T>, Integer> windowedStage, ProudContext context) throws UnsupportedSpaceException {
        throw new UnsupportedSpaceException(ProudSpaceOption.MultiQueryMultiParamsMultiWindowParams, algorithm);
    }
}
