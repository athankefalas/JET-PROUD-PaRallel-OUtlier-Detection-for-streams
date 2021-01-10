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
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.SlicingProudData;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.algorithms.AnyProudAlgorithmExecutor;
import edu.auth.jetproud.proud.algorithms.ProudKeyedWindow;
import edu.auth.jetproud.proud.algorithms.exceptions.UnsupportedSpaceException;
import edu.auth.jetproud.proud.algorithms.functions.ProudComponentBuilder;
import edu.auth.jetproud.proud.distributables.KeyedStateHolder;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

public class SlicingProudAlgorithmExecutor extends AnyProudAlgorithmExecutor<SlicingProudData>
{
    public static final Long OUTLIERS_TRIGGER = Long.MIN_VALUE;

    public static class SlicingState implements Serializable {
        public ConcurrentHashMap<Long, MTree<SlicingProudData>> trees;
        public ConcurrentHashMap<Long, CopyOnWriteArraySet<Integer>> triggers;

        public SlicingState() {
            this(new HashMap<>(), new HashMap<>());
        }

        public SlicingState(HashMap<Long, MTree<SlicingProudData>> trees, HashMap<Long, CopyOnWriteArraySet<Integer>> triggers) {
            this.trees = new ConcurrentHashMap<>(trees);
            this.triggers = new ConcurrentHashMap<>(triggers);
        }
    }

    public SlicingProudAlgorithmExecutor(ProudContext proudContext) {
        super(proudContext, ProudAlgorithmOption.Slicing);
    }

    @Override
    protected <D extends AnyProudData> SlicingProudData transform(D point) {
        return new SlicingProudData(point);
    }

    @Override
    public List<ProudSpaceOption> supportedSpaceOptions() {
        return Lists.of(ProudSpaceOption.Single);
    }

    @Override
    protected StreamStage<Tuple<Long, OutlierQuery>> processSingleSpace(StreamStage<KeyedWindowResult<Integer, List<Tuple<Integer, SlicingProudData>>>> windowedStage) throws UnsupportedSpaceException {
        final long windowSize = proudContext.internalConfiguration().getCommonW();
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

        return windowedStage.flatMapStateful(()-> KeyedStateHolder.<String, SlicingState>create(),
                (stateHolder, window) -> {
                    // Metrics & Statistics
                    SlideMetricsRecorder metricsRecorder = startRecordingMetrics();

                    // Detect outliers and add them to outliers accumulator
                    List<Tuple<Long, OutlierQuery>> outliers = Lists.make();
                    int partition = window.getKey();

                    long windowStart = window.start();
                    long windowEnd = window.end();
                    long latestSlide = windowEnd - slide;

                    final String STATE_KEY = "STATE_"+partition;

                    SlicingState current = stateHolder.getOrDefault(STATE_KEY, null);

                    List<SlicingProudData> elements = window.getValue().stream()
                            .map(Tuple::getSecond)
                            .collect(Collectors.toList());

                    // Create MTree
                    SplitFunction<SlicingProudData> splitFunction = SplitFunction.composedOf(
                            PromotionFunction.minMax(),
                            PartitionFunction.balanced()
                    );

                    MTree<SlicingProudData> mTree = new MTree<>(k, DistanceFunction.euclidean(), splitFunction);

                    if(current == null) {
                        HashMap<Long, CopyOnWriteArraySet<Integer>> triggers = new HashMap<>();
                        triggers.put(OUTLIERS_TRIGGER, new CopyOnWriteArraySet<>());

                        // Create trigger sets for window slides
                        long nextSlide = windowStart;

                        while(nextSlide <= windowEnd - slide) {
                            triggers.put(nextSlide, new CopyOnWriteArraySet<>());
                            nextSlide += slide;
                        }

                        for (SlicingProudData el:elements) {
                            mTree.addOrCache(el);
                        }

                        HashMap<Long, MTree<SlicingProudData>> trees = new HashMap<>();
                        trees.put(latestSlide, mTree);

                        current = new SlicingState(trees, triggers);
                        stateHolder.put(STATE_KEY, current);
                    } else {
                        // Add elements to MTree
                        elements.stream()
                                .filter((el)->el.arrival >= windowEnd - slide)
                                .forEach(mTree::addOrCache);

                        long max = current.triggers.keySet().stream()
                                .max(Long::compare)
                                .orElse(windowStart) + slide;

                        while (max <= windowEnd - slide) {
                            current.triggers.put(max, new CopyOnWriteArraySet<>());
                            max += slide;
                        }

                        current.trees.put(latestSlide, mTree);
                    }

                    ProudKeyedWindow<SlicingProudData> windowRef = new ProudKeyedWindow<>(partition, windowStart, windowEnd, elements);
                    final Slicing slicing = new Slicing(windowRef, outlierQuery, current);

                    //Trigger leftover slides
                    List<Long> slowTriggers = current.triggers.keySet().stream()
                            .filter((it)-> it < windowStart && !it.equals(OUTLIERS_TRIGGER))
                            .collect(Collectors.toList());

                    for (Long slowTrigger: slowTriggers) {
                        final CopyOnWriteArraySet<Integer> triggerPointIds = new CopyOnWriteArraySet<>(current.triggers.get(slowTrigger));

                        List<SlicingProudData> triggerPoints = elements.stream()
                                .filter((it)-> triggerPointIds.contains(it.id))
                                .collect(Collectors.toList());

                        for (SlicingProudData el:triggerPoints) {
                            slicing.triggerPoint(el);
                        }

                        current.triggers.remove(slowTrigger);

                    }

                    //Insert new points
                    elements.stream()
                            .filter((it)->it.arrival >= windowEnd - slide && it.flag == 0)
                            .forEach(slicing::insertPoint);

                    //Trigger previous outliers
                    List<Integer> triggeredOutliers = Lists.copyOf(current.triggers.get(OUTLIERS_TRIGGER));
                    current.triggers.get(OUTLIERS_TRIGGER).clear();

                    elements.stream()
                            .filter((it)-> triggeredOutliers.contains(it.id))
                            .forEach(slicing::triggerPoint);

                    // Find and Report outliers
                    List<SlicingProudData> outlierData = elements.stream()
                            .filter((it)-> !it.safe_inlier.get() && it.flag == 0)
                            .filter((it)-> {
                                return it.count_after.get() + it.slices_before.keySet().stream()
                                        .filter((key)-> key >= windowStart)
                                        .mapToInt((key)->it.slices_before.get(key))
                                        .sum() < k;
                            })
                            .collect(Collectors.toList());

                    int outliersCount = outlierData.size();
                    OutlierQuery queryCopy = outlierQuery.withOutlierCount(outliersCount);
                    outliers.add(new Tuple<>(windowEnd, queryCopy));

                    //Trigger expiring list
                    current.trees.remove(windowStart);
                    List<Integer> triggeredIds = Lists.copyOf(current.triggers.get(windowStart));
                    current.triggers.remove(windowStart);

                    elements.stream()
                            .filter((it)->triggeredIds.contains(it.id))
                            .forEach(slicing::triggerPoint);

                    // Metrics & Statistics
                    stopRecordingMetrics(metricsRecorder);

                    // Return results
                    return Traversers.traverseIterable(outliers);
                });
    }

    private static class Slicing implements Serializable
    {
        public ProudKeyedWindow<SlicingProudData> window;
        public OutlierQuery query;
        public SlicingState state;

        public Slicing(ProudKeyedWindow<SlicingProudData> window, OutlierQuery query, SlicingState state) {
            this.window = window;
            this.query = query;
            this.state = state;
        }

        public void triggerPoint(SlicingProudData point) {
            long slideDuration = query.slide;
            int k = query.kNeighbours;
            double r = query.range;

            long nextSlide;

            // Find starting slide
            if (point.last_check.get() != 0L)
                nextSlide = point.last_check.get() + slideDuration;
            else
                nextSlide = getSlide(point.arrival) + slideDuration;

            //Find number of neighbors
            int neighbourCount = point.count_after.get() + point.slices_before.keySet().stream()
                    .filter((it) -> it >= window.start + slideDuration)
                    .mapToInt((it) -> point.slices_before.get(it))
                    .sum();

            while (neighbourCount < k && nextSlide <= window.end - slideDuration) {
                MTree<SlicingProudData> mTree = state.trees.getOrDefault(nextSlide, null);

                if (mTree != null) {
                    List<SlicingProudData> nearest = mTree.findNearestInRange(point, r);
                    int nodeCount = nearest.size();

                    point.count_after.addAndGet(nodeCount);
                    neighbourCount += nodeCount;

                    if (point.count_after.get() >= k)
                        point.safe_inlier.set(true);
                }

                point.last_check.set(nextSlide);
                nextSlide += slideDuration;
            }


            if (neighbourCount < k) {
                state.triggers.get(SlicingProudAlgorithmExecutor.OUTLIERS_TRIGGER)
                        .add(point.id);
            }

        }

        private long getSlide(long arrivalTime) {
            long first = arrivalTime - window.start;
            long div = first / query.slide;
            int intDiv = (int) div;

            return window.start + ((long) intDiv * query.slide);
        }

        public void insertPoint(SlicingProudData point) {
            long slide = query.slide;
            int k = query.kNeighbours;
            double r = query.range;

            int neighbourCount = 0;
            long nextSlide = window.end - slide;

            while (neighbourCount < k && nextSlide >= window.start) {
                MTree<SlicingProudData> mTree = state.trees.getOrDefault(nextSlide, null);

                if (mTree != null) {
                    List<SlicingProudData> items = mTree.findNearestInRange(point, r);

                    if (!items.isEmpty()) {
                        state.triggers.get(nextSlide).add(point.id);
                    }

                    for(SlicingProudData node:items) {
                        if (nextSlide == window.end - slide) {
                            if (node.id != point.id) {
                                point.count_after.addAndGet(1);
                                neighbourCount++;
                            }
                        } else {
                            point.slices_before.put(nextSlide, point.slices_before.getOrDefault(nextSlide, 0) + 1);
                            neighbourCount++;
                        }
                    }


                    if (nextSlide == window.end - slide && neighbourCount >= k)
                        point.safe_inlier.set(true);
                }

                nextSlide -= slide;
            }


            //If it is an outlier insert into trigger list
            if (neighbourCount < k) {
                state.triggers.get(SlicingProudAlgorithmExecutor.OUTLIERS_TRIGGER)
                        .add(point.id);
            }

        }
    }
}
