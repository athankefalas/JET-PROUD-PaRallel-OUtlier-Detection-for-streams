package edu.auth.jetproud.proud.algorithms.executors;

import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.application.parameters.data.ProudSpaceOption;
import edu.auth.jetproud.datastructures.mtree.MTree;
import edu.auth.jetproud.datastructures.mtree.distance.DistanceFunction;
import edu.auth.jetproud.datastructures.mtree.partition.PartitionFunction;
import edu.auth.jetproud.datastructures.mtree.promotion.PromotionFunction;
import edu.auth.jetproud.datastructures.mtree.split.SplitFunction;
import edu.auth.jetproud.model.AdvancedProudData;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.algorithms.AnyProudAlgorithmExecutor;
import edu.auth.jetproud.proud.algorithms.exceptions.UnsupportedSpaceException;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.distributables.KeyedStateHolder;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class AdvancedExtendedProudAlgorithmExecutor extends AnyProudAlgorithmExecutor<AdvancedProudData>
{

    public static class AdvancedExtendedState implements Serializable {
        public MTree<AdvancedProudData> mTree;
        public ConcurrentHashMap<Integer, AdvancedProudData> map;

        public AdvancedExtendedState() {
        }

        public AdvancedExtendedState(MTree<AdvancedProudData> mTree, HashMap<Integer, AdvancedProudData> map) {
            this.mTree = mTree;
            this.map = new ConcurrentHashMap<>(map);
        }
    }

    public AdvancedExtendedProudAlgorithmExecutor(ProudContext proudContext) {
        super(proudContext, ProudAlgorithmOption.AdvancedExtended);
    }

    @Override
    protected <D extends AnyProudData> AdvancedProudData transform(D point) {
        return new AdvancedProudData(point);
    }

    @Override
    public List<ProudSpaceOption> supportedSpaceOptions() {
        return Lists.of(ProudSpaceOption.Single);
    }

    @Override
    protected StreamStage<Tuple<Long, OutlierQuery>> processSingleSpace(StreamStage<KeyedWindowResult<Integer, List<Tuple<Integer, AdvancedProudData>>>> windowedStage) throws UnsupportedSpaceException {
        // Create Outlier Query - Queries
        int w = proudContext.configuration().getWindowSizes().get(0);
        int s = proudContext.configuration().getSlideSizes().get(0);
        double r = proudContext.configuration().getRNeighbourhood().get(0);
        int k = proudContext.configuration().getKNeighbours().get(0);

        final OutlierQuery outlierQuery = new OutlierQuery(r,k,w,s);

        final int slide = outlierQuery.slide;
        final int K = outlierQuery.kNeighbours;
        final double R = outlierQuery.range;

        return windowedStage.flatMapStateful(()->KeyedStateHolder.<String, AdvancedExtendedState>create(),
                (stateHolder, window)-> {
                    // Metrics & Statistics
                    SlideMetricsRecorder metricsRecorder = startRecordingMetrics();

                    // Detect outliers and add them to outliers accumulator
                    int partition = window.getKey();

                    long windowStart = window.start();
                    long windowEnd = window.end();

                    final String STATE_KEY = "STATE_"+partition;

                    AdvancedExtendedState current = stateHolder.getOrDefault(STATE_KEY, null);

                    List<AdvancedProudData> elements = window.getValue().stream()
                            .map(Tuple::getSecond)
                            .collect(Collectors.toList());

                    if (current == null) {
                        SplitFunction<AdvancedProudData> splitFunction = SplitFunction.composedOf(
                                PromotionFunction.minMax(),
                                PartitionFunction.balanced()
                        );

                        MTree<AdvancedProudData> mTree = new MTree<>(k, DistanceFunction.euclidean(), splitFunction);
                        current = new AdvancedExtendedState(mTree, new HashMap<>());

                        for(AdvancedProudData el:elements) {
                            current.mTree.addOrCache(el);
                            current.map.put(el.id, el);
                        }

                        // Filter non-expired
                        elements = elements.stream()
                                .filter((it)->it.arrival >= windowEnd - slide)
                                .collect(Collectors.toList());

                    } else {
                        // Filter non-expired
                        elements = elements.stream()
                                .filter((it)->it.arrival >= windowEnd - slide)
                                .collect(Collectors.toList());

                        for (AdvancedProudData el:elements) {
                            current.mTree.addOrCache(el);
                            current.map.put(el.id, el);
                        }
                    }

                    for (AdvancedProudData el: elements) {
                        List<AdvancedProudData> treeQuery = current.mTree.findNearestOrCachedInRange(el, r);

                        for (AdvancedProudData node:treeQuery) {

                            if (node.id == el.id)
                                continue;

                            if (node.arrival < windowEnd - slide) {

                                if (el.flag == 0) {
                                    current.map.get(el.id).insert_nn_before(node.arrival, k);
                                }

                                if (node.flag == 0) {
                                    current.map.get(node.id).count_after++;

                                    if (current.map.get(node.id).count_after >= k)
                                        current.map.get(node.id).safe_inlier = true;
                                }

                            } else {

                                if (el.flag == 0) {
                                    current.map.get(el.id).count_after++;

                                    if (current.map.get(el.id).count_after >= k)
                                        current.map.get(el.id).safe_inlier = true;
                                }
                            }
                        }
                    }

                    // Add outliers to accumulator
                    int outliersCount = 0;

                    for (AdvancedProudData el:current.map.values()) {
                        if (el.flag == 0 && !el.safe_inlier) {
                            long nnBefore = el.nn_before.stream()
                                    .filter((it)->it >= windowStart)
                                    .count();

                            if (el.count_after + nnBefore < k)
                                outliersCount++;
                        }
                    }

                    // Remove expiring and flagged objects from MTree
                    List<AdvancedProudData> toRemove = window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((el) -> el.arrival < windowStart + slide)
                            .collect(Collectors.toList());

                    for (AdvancedProudData item:toRemove) {
                        current.mTree.remove(item);
                        current.map.remove(item.id);
                    }

                    // Update state - If state is updated results are not correct
                    //stateHolder.put(STATE_KEY, current);

                    // Metrics & Statistics
                    stopRecordingMetrics(metricsRecorder);

                    // Return results
                    OutlierQuery queryCopy = outlierQuery.withOutlierCount(outliersCount);
                    return Traversers.singleton(new Tuple<>(windowEnd, queryCopy));
                });
    }

}
