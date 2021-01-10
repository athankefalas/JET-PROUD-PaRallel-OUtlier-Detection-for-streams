package edu.auth.jetproud.proud.algorithms.executors.custom;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.application.parameters.data.ProudSpaceOption;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.algorithms.AnyProudAlgorithmExecutor;
import edu.auth.jetproud.proud.algorithms.ProudKeyedWindow;
import edu.auth.jetproud.proud.algorithms.exceptions.UnsupportedSpaceException;
import edu.auth.jetproud.proud.algorithms.executors.AdvancedExtendedProudAlgorithmExecutor;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.distributables.KeyedStateHolder;
import edu.auth.jetproud.utils.ExceptionUtils;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CustomFunctionProudAlgorithmExecutor<Data extends AnyProudData, State extends Serializable> extends AnyProudAlgorithmExecutor<Data>
{
    private FunctionEx<AnyProudData, Data> converter;
    private BiFunctionEx<ProudKeyedWindow<Data>, Map<String,State>, OutlierQuery> outlierDetectionFunction;

    public CustomFunctionProudAlgorithmExecutor(ProudContext proudContext, FunctionEx<AnyProudData, Data> converter, BiFunctionEx<ProudKeyedWindow<Data>, Map<String,State>, OutlierQuery> outlierDetectionFunction) {
        super(proudContext, ProudAlgorithmOption.UserDefined);

        this.converter = converter;
        this.outlierDetectionFunction = outlierDetectionFunction;
    }

    @Override
    public List<ProudSpaceOption> supportedSpaceOptions() {
        return Lists.of(proudContext.configuration().getSpace());
    }

    @Override
    protected <D1 extends AnyProudData> Data transform(D1 point) {
        try {
            return converter.applyEx(point);
        } catch (Exception e) {
            throw ExceptionUtils.sneaky(e);
        }
    }

    @Override
    protected StreamStage<Tuple<Long, OutlierQuery>> processSingleSpace(StreamStage<KeyedWindowResult<Integer, List<Tuple<Integer, Data>>>> windowedStage) throws UnsupportedSpaceException {
        if (proudContext.configuration().getSpace() != ProudSpaceOption.Single)
            return super.processSingleSpace(windowedStage);

        return windowedStage.flatMapStateful(()-> KeyedStateHolder.<String, State>create(),
                (stateHolder, window)-> {
                    // Metrics & Statistics
                    SlideMetricsRecorder metricsRecorder = startRecordingMetrics();

                    // Detect outliers and add them to outliers accumulator
                    int partition = window.getKey();

                    long windowStart = window.start();
                    long windowEnd = window.end();

                    List<Data> elements = window.getValue().stream()
                            .map(Tuple::getSecond)
                            .collect(Collectors.toList());

                    // Create window
                    ProudKeyedWindow<Data> proudWindow = new ProudKeyedWindow<>(partition, windowStart, windowEnd, elements);

                    // Call outlier detection function
                    OutlierQuery outliersQuery = this.outlierDetectionFunction.applyEx(proudWindow, stateHolder);

                    // Metrics & Statistics
                    stopRecordingMetrics(metricsRecorder);

                    // Return results
                    return Traversers.singleton(new Tuple<>(windowEnd, outliersQuery));
                });
    }

    @Override
    protected StreamStage<Tuple<Long, OutlierQuery>> processMultiQueryParamsSpace(StreamStage<KeyedWindowResult<Integer, List<Tuple<Integer, Data>>>> windowedStage) throws UnsupportedSpaceException {
        if (proudContext.configuration().getSpace() != ProudSpaceOption.MultiQueryMultiParams)
            return super.processSingleSpace(windowedStage);

        return windowedStage.flatMapStateful(()-> KeyedStateHolder.<String, State>create(),
                (stateHolder, window)-> {
                    // Metrics & Statistics
                    SlideMetricsRecorder metricsRecorder = startRecordingMetrics();

                    // Detect outliers and add them to outliers accumulator
                    int partition = window.getKey();

                    long windowStart = window.start();
                    long windowEnd = window.end();

                    List<Data> elements = window.getValue().stream()
                            .map(Tuple::getSecond)
                            .collect(Collectors.toList());

                    // Create window
                    ProudKeyedWindow<Data> proudWindow = new ProudKeyedWindow<>(partition, windowStart, windowEnd, elements);

                    // Call outlier detection function
                    OutlierQuery outliersQuery = this.outlierDetectionFunction.applyEx(proudWindow, stateHolder);

                    // Metrics & Statistics
                    stopRecordingMetrics(metricsRecorder);

                    // Return results
                    return Traversers.singleton(new Tuple<>(windowEnd, outliersQuery));
                });
    }

    @Override
    protected StreamStage<Tuple<Long, OutlierQuery>> processMultiQueryParamsMultiWindowParamsSpace(StreamStage<KeyedWindowResult<Integer, List<Tuple<Integer, Data>>>> windowedStage) throws UnsupportedSpaceException {
        if (proudContext.configuration().getSpace() != ProudSpaceOption.MultiQueryMultiParamsMultiWindowParams)
            return super.processSingleSpace(windowedStage);

        return windowedStage.flatMapStateful(()-> KeyedStateHolder.<String, State>create(),
                (stateHolder, window)-> {
                    // Metrics & Statistics
                    SlideMetricsRecorder metricsRecorder = startRecordingMetrics();

                    // Detect outliers and add them to outliers accumulator
                    int partition = window.getKey();

                    long windowStart = window.start();
                    long windowEnd = window.end();

                    List<Data> elements = window.getValue().stream()
                            .map(Tuple::getSecond)
                            .collect(Collectors.toList());

                    // Create window
                    ProudKeyedWindow<Data> proudWindow = new ProudKeyedWindow<>(partition, windowStart, windowEnd, elements);

                    // Call outlier detection function
                    OutlierQuery outliersQuery = this.outlierDetectionFunction.applyEx(proudWindow, stateHolder);

                    // Metrics & Statistics
                    stopRecordingMetrics(metricsRecorder);

                    // Return results
                    return Traversers.singleton(new Tuple<>(windowEnd, outliersQuery));
                });
    }
}
