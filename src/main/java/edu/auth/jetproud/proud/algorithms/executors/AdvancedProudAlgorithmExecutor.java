package edu.auth.jetproud.proud.algorithms.executors;

import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.application.parameters.data.ProudSpaceOption;
import edu.auth.jetproud.datastructures.mtree.MTree;
import edu.auth.jetproud.datastructures.mtree.distance.DistanceFunction;
import edu.auth.jetproud.datastructures.mtree.partition.PartitionFunction;
import edu.auth.jetproud.datastructures.mtree.promotion.PromotionFunction;
import edu.auth.jetproud.datastructures.mtree.split.SplitFunction;
import edu.auth.jetproud.model.AdvancedProudData;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.meta.OutlierMetadata;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.algorithms.AnyProudAlgorithmExecutor;
import edu.auth.jetproud.proud.algorithms.exceptions.UnsupportedSpaceException;
import edu.auth.jetproud.proud.algorithms.functions.ProudComponentBuilder;
import edu.auth.jetproud.proud.distributables.KeyedStateHolder;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class AdvancedProudAlgorithmExecutor extends AnyProudAlgorithmExecutor<AdvancedProudData>
{

    public static class AdvancedState implements Serializable
    {
        public MTree<AdvancedProudData> mTree;
        public ConcurrentHashMap<Integer, AdvancedProudData> map;

        public AdvancedState() {
        }

        public AdvancedState(MTree<AdvancedProudData> mTree, HashMap<Integer, AdvancedProudData> map) {
            this.mTree = mTree;
            this.map = new ConcurrentHashMap<>(map);
        }
    }

    public AdvancedProudAlgorithmExecutor(ProudContext proudContext) {
        super(proudContext, ProudAlgorithmOption.Advanced);
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
        final long windowSize = proudContext.internalConfiguration().getCommonW();
        final long slideSize = proudContext.internalConfiguration().getCommonS();
        final int partitionsCount = proudContext.internalConfiguration().getPartitions();
        ProudComponentBuilder components = ProudComponentBuilder.create(proudContext);

        // Create Outlier Query - Queries
        int w = proudContext.configuration().getWindowSizes().get(0);
        int s = proudContext.configuration().getSlideSizes().get(0);
        double r = proudContext.configuration().getRNeighbourhood().get(0);
        int k = proudContext.configuration().getKNeighbours().get(0);

        final OutlierQuery outlierQuery = new OutlierQuery(r,k,w,s);

        final int slide = outlierQuery.slide;
        final int K = outlierQuery.kNeighbours;
        final double R = outlierQuery.range;

        StreamStage<AdvancedProudData> detectedOutliersStage = windowedStage.flatMapStateful(()-> KeyedStateHolder.<String, AdvancedState>create(),
            (stateHolder, window) -> {
                // Metrics & Statistics
                SlideMetricsRecorder metricsRecorder = startRecordingMetrics();

                // Detect outliers and add them to outliers accumulator
                List<AdvancedProudData> outliers = Lists.make();
                int partition = window.getKey();

                long windowStart = window.start();
                long windowEnd = window.end();

                final String STATE_KEY = "STATE_"+partition;
                AdvancedState current = stateHolder.getOrDefault(STATE_KEY, null);

                List<AdvancedProudData> elements = window.getValue().stream()
                        .map(Tuple::getSecond)
                        .collect(Collectors.toList());

                // Evict old elements
                elements = Advanced.evict(elements, windowStart, windowEnd, slide);

                List<AdvancedProudData> windowItems = Lists.copyOf(elements);

                if (current == null) {
                    SplitFunction<AdvancedProudData> splitFunction = SplitFunction.composedOf(
                            PromotionFunction.minMax(),
                            PartitionFunction.balanced()
                    );

                    MTree<AdvancedProudData> mTree = new MTree<>(K, DistanceFunction.euclidean(), splitFunction);
                    current = new AdvancedState(mTree, new HashMap<>());

                    for(AdvancedProudData el:elements) {
                        current.mTree.addOrCache(el);
                        current.map.put(el.id, el);
                    }

                } else {
                    AdvancedState finalCurrent = current;

                    // Filter non-expired
                    elements.stream()
                            .filter((it)->it.arrival >= windowEnd - slide)
                            .forEach((el)->{
                                finalCurrent.mTree.addOrCache(el);
                                finalCurrent.map.put(el.id, el);
                            });
                }

                AdvancedState finalCurrent = current;

                // Get Neighbours
                elements.stream()
                        .filter((p) -> p.arrival >= (windowEnd - slide))
                        .forEach((el)->{
                            List<AdvancedProudData> treeQuery = finalCurrent.mTree.findNearestOrCachedInRange(el, R);

                            for (AdvancedProudData node:treeQuery) {

                                if (node.id == el.id)
                                    continue;

                                if (node.arrival < windowEnd - slide) {

                                    finalCurrent.map.get(el.id).insert_nn_before(node.arrival, k);
                                    finalCurrent.map.get(node.id).count_after++;

                                    if (finalCurrent.map.get(node.id).count_after >= k)
                                        finalCurrent.map.get(node.id).safe_inlier = true;

                                } else {

                                    if (el.flag == 0) {
                                        finalCurrent.map.get(el.id).count_after++;

                                        if (finalCurrent.map.get(el.id).count_after >= k)
                                            finalCurrent.map.get(el.id).safe_inlier = true;
                                    }
                                }
                            }
                        });

                // Add outliers to accumulator
                outliers.addAll(current.map.values().stream()
                        .map(AdvancedProudData::copy)
                        .collect(Collectors.toList())
                );

                // Remove expiring and flagged objects from MTree
                List<AdvancedProudData> toRemove = elements.stream()
                        .filter((el) -> el.arrival < windowStart + slide || el.flag == 1)
                        .collect(Collectors.toList());

                for (AdvancedProudData item:toRemove) {
                    current.mTree.remove(item);
                    current.map.remove(item.id);
                }

                // Update state
                //stateHolder.put(STATE_KEY, current);

                // Metrics & Statistics
                stopRecordingMetrics(metricsRecorder);

                //Return outliers
                return Traversers.traverseIterable(outliers);
            });

        // Group Metadata
        return detectedOutliersStage
                .window(WindowDefinition.tumbling(slideSize))
                .groupingKey((it)->it.id % partitionsCount)
                .aggregate(components.metaWindowAggregator())
                .flatMapStateful(()-> KeyedStateHolder.<String, OutlierMetadata<AdvancedProudData>>create(),
                        (stateHolder, window) -> {
                            int windowKey = window.getKey();

                            long windowStart = window.start();
                            long windowEnd = window.end();

                            List<AdvancedProudData> elements = window.getValue();

                            final String METADATA_KEY = "METADATA_"+windowKey;
                            OutlierMetadata<AdvancedProudData> current = stateHolder.get(METADATA_KEY);

                            // Create / Update state Map
                            if(current == null) {
                                Map<Integer,AdvancedProudData> outliersMap = new HashMap<>();

                                for (AdvancedProudData el:elements) {
                                    AdvancedProudData oldElement = outliersMap.getOrDefault(el.id, null);
                                    AdvancedProudData combined = Advanced.combineNewElements(oldElement, el, k);
                                    outliersMap.put(el.id, combined);
                                }

                                current = new OutlierMetadata<>(outliersMap);
                            } else {

                                // Remove old elements
                                List<Integer> idsToRemove = Lists.make();

                                for (AdvancedProudData el: current.values()) {
                                    if (el.arrival < windowEnd - w) {
                                        idsToRemove.add(el.id);
                                    }
                                }

                                for (Integer id : idsToRemove) {
                                    current.remove(id);
                                }

                                // Then insert or combine elements
                                for (AdvancedProudData el:elements) {
                                    AdvancedProudData oldEl = current.getOrDefault(el.id, null);

                                    if (el.arrival >= windowEnd - slide) {
                                        AdvancedProudData newValue = Advanced.combineNewElements(oldEl, el, k);
                                        current.put(el.id, newValue);
                                    } else {
                                        if (oldEl != null) {
                                            AdvancedProudData newValue = Advanced.combineOldElements(oldEl, el, k);
                                            current.put(el.id, newValue);
                                        }
                                    }
                                }
                            }

                            List<AdvancedProudData> outlierValues = Lists.copyOf(current.values());

                            stateHolder.put(METADATA_KEY, current);

                            int outliers = 0;

                            for (AdvancedProudData el:outlierValues) {
                                if (el.safe_inlier)
                                    continue;

                                long nnBefore = el.nn_before.stream()
                                        .filter((it)->it >= windowEnd - w)
                                        .count();

                                if (nnBefore + el.count_after < k)
                                    outliers++;
                            }

                            // Return results
                            OutlierQuery queryCopy = outlierQuery.withOutlierCount(outliers);
                            return Traversers.singleton(new Tuple<>(windowEnd, queryCopy));
                        }
                );
    }


    private static class Advanced implements Serializable
    {
        public static List<AdvancedProudData> evict(List<AdvancedProudData> windowData,long windowStart, long windowEnd, long slide) {
            List<AdvancedProudData> evicted = Lists.copyOf(windowData);

            evicted.removeIf((it)->{
                return it.flag == 1 && it.arrival >= windowStart && it.arrival < windowEnd - slide;
            });

            return evicted;
        }

        public static AdvancedProudData combineOldElements(AdvancedProudData one, AdvancedProudData other, int k) {
            one.count_after = other.count_after;
            one.safe_inlier = other.safe_inlier;

            return one;
        }

        public static AdvancedProudData combineNewElements(AdvancedProudData one, AdvancedProudData other, int k) {
            if (one == null || other == null) {
                return edu.auth.jetproud.utils.Utils.firstNonNull(one, other);
            }

            if (one.flag == 1 && other.flag == 1) {
                other.nn_before.forEach((it)->one.insert_nn_before(it, k));
                return one;
            } else if (other.flag == 0) {
                one.nn_before.forEach((it)->other.insert_nn_before(it, k));
                return other;
            } else if (one.flag == 0) {
                other.nn_before.forEach((it)->one.insert_nn_before(it, k));
                return one;
            }

            return null;
        }
    }

}
