package edu.auth.jetproud.proud.algorithms.executors;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.datastructures.mtree.MTree;
import edu.auth.jetproud.datastructures.mtree.ResultItem;
import edu.auth.jetproud.datastructures.mtree.distance.DistanceFunction;
import edu.auth.jetproud.datastructures.mtree.partition.PartitionFunction;
import edu.auth.jetproud.datastructures.mtree.promotion.PromotionFunction;
import edu.auth.jetproud.datastructures.mtree.split.SplitFunction;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.SlicingProudData;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.ProudContext;
import edu.auth.jetproud.proud.algorithms.AnyProudAlgorithmExecutor;
import edu.auth.jetproud.proud.algorithms.KeyedWindow;
import edu.auth.jetproud.proud.algorithms.exceptions.UnsupportedSpaceException;
import edu.auth.jetproud.proud.algorithms.functions.ProudComponentBuilder;
import edu.auth.jetproud.proud.distributables.DistributedMap;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SlicingProudAlgorithmExecutor extends AnyProudAlgorithmExecutor<SlicingProudData>
{
    public static final Long OUTLIERS_TRIGGER = -1L;
    public static final String DATA_STATE = "DATA_STATE";

    public static class SlicingState {
        public HashMap<Long, MTree<SlicingProudData>> trees;
        public HashMap<Long, Set<Integer>> triggers;

        public SlicingState() {
        }

        public SlicingState(HashMap<Long, MTree<SlicingProudData>> trees, HashMap<Long, Set<Integer>> triggers) {
            this.trees = trees;
            this.triggers = triggers;
        }
    }

    public SlicingProudAlgorithmExecutor(ProudContext proudContext) {
        super(proudContext, ProudAlgorithmOption.Slicing);
    }

    @Override
    public void createDistributableData() {
        super.createDistributableData();
        DistributedMap<String, SlicingState> stateMap = new DistributedMap<>(DATA_STATE);
    }

    @Override
    protected <D extends AnyProudData> SlicingProudData transform(D point) {
        return new SlicingProudData(point);
    }

    @Override
    protected Object processSingleSpace(StreamStage<KeyedWindowResult<Integer, List<Tuple<Integer, SlicingProudData>>>> windowedStage) throws UnsupportedSpaceException {
        // TODO: Impl
        // Initialize distributed stateful data
        createDistributableData();

        final DistributedMap<String, SlicingState> stateMap = new DistributedMap<>(DATA_STATE);

        final long windowSize = proudContext.getProudInternalConfiguration().getCommonW();
        final int partitionsCount = proudContext.getProudInternalConfiguration().getPartitions();
        ProudComponentBuilder components = ProudComponentBuilder.create(proudContext);

        // Create Outlier Query - Queries
        int w = proudContext.getProudConfiguration().getWindowSizes().get(0);
        int s = proudContext.getProudConfiguration().getSlideSizes().get(0);
        double r = proudContext.getProudConfiguration().getRNeighbourhood().get(0);
        int k = proudContext.getProudConfiguration().getKNeighbours().get(0);

        final OutlierQuery outlierQuery = new OutlierQuery(r,k,w,s);

        final int slide = outlierQuery.s;
        final int K = outlierQuery.k;
        final double R = outlierQuery.r;

        StreamStage<List<Tuple<Long,OutlierQuery>>> detectOutliersStage = windowedStage.rollingAggregate(
                components.outlierDetection((outliers, window)->{
                    // Detect outliers and add them to outliers accumulator
                    int partition = window.getKey();

                    long windowStart = window.start();
                    long windowEnd = window.end();
                    long latestSlide = windowEnd - slide;

                    final String STATE_KEY = "STATE";

                    SlicingState current = stateMap.getOrDefault(STATE_KEY, null);

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
                        HashMap<Long, Set<Integer>> triggers = new HashMap<>();
                        triggers.put(OUTLIERS_TRIGGER, new HashSet<>());

                        // Create trigger sets for window slides
                        long nextSlide = windowStart;
                        while(nextSlide <= windowEnd - slide) {
                            triggers.put(nextSlide, new HashSet<>());
                            nextSlide += slide;
                        }

                        for (SlicingProudData el:elements) {
                            mTree.add(el);
                        }

                        HashMap<Long, MTree<SlicingProudData>> trees = new HashMap<>();
                        trees.put(latestSlide, mTree);

                        current = new SlicingState(trees, triggers);
                        stateMap.put(STATE_KEY, current);
                    } else {
                        List<SlicingProudData> activeElements = elements.stream()
                                .filter((el)->el.arrival >= windowEnd - slide)
                                .collect(Collectors.toList());

                        activeElements.forEach(mTree::add);

                        long max = current.triggers.keySet().stream()
                                .max(Long::compare)
                                .orElse(windowStart);

                        while (max <= windowEnd - slide) {
                            current.triggers.put(max, new HashSet<>());
                            max += slide;
                        }

                        current.trees.put(latestSlide, mTree);
                    }

                    // Declare common vars needed by AlgorithmUtils class
                    KeyedWindow<SlicingProudData> windowRef = new KeyedWindow<>(partition, windowStart, windowEnd, elements);
                    final Slicing slicing = new Slicing(windowRef, outlierQuery, current);

                    //Trigger leftover slides
                    List<Long> slowTriggers = current.triggers.keySet().stream()
                            .filter((it)-> it < windowStart && !it.equals(OUTLIERS_TRIGGER))
                            .collect(Collectors.toList());

                    for (Long slowTrigger: slowTriggers) {
                        List<Integer> triggerPointIds = Lists.copyOf(current.triggers.get(slowTrigger));

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
                            .filter((it)->it.arrival >=windowEnd - slide && it.flag == 0)
                            .forEach(slicing::insertPoint);

                    //Trigger previous outliers
                    List<Integer> triggeredOutliers = Lists.copyOf(current.triggers.get(OUTLIERS_TRIGGER));
                    current.triggers.get(OUTLIERS_TRIGGER).clear();

                    elements.stream()
                            .filter((it)-> triggeredOutliers.contains(it.id))
                            .forEach(slicing::triggerPoint);

                    // Find and Report outliers
                    List<SlicingProudData> outlierData = elements.stream()
                            .filter((it)-> {
                                return it.flag == 0 && !it.safe_inlier
                                        && it.count_after + it.slices_before.keySet().stream()
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
                })
        );


        // TODO: Return the proper stream stage
        //flatten here ??? and then to pipeline Sink

        //return final stage
        return super.processSingleSpace(windowedStage);
    }

    private static class Slicing implements Serializable
    {
        public KeyedWindow<SlicingProudData> window;
        public OutlierQuery query;
        public SlicingState state;

        public Slicing(KeyedWindow<SlicingProudData> window, OutlierQuery query, SlicingState state) {
            this.window = window;
            this.query = query;
            this.state = state;
        }

        public void triggerPoint(SlicingProudData point) {
            long slideDuration = query.s;
            int k = query.k;
            double r = query.r;

            long nextSlide;

            if (point.last_check != 0L)
                nextSlide = point.last_check + slideDuration;
            else
                nextSlide = getSlide(point.arrival, slideDuration);

            //Find number of neighbors
            int neighbourCount = point.count_after + point.slices_before.keySet().stream()
                    .filter((it) -> it > window.start + slideDuration)
                    .mapToInt((it) -> point.slices_before.get(it))
                    .sum();

            while (neighbourCount < k && nextSlide <= window.end - slideDuration) {
                MTree<SlicingProudData> mTree = state.trees.getOrDefault(nextSlide, null);

                if (mTree != null) {
                    MTree<SlicingProudData>.Query treeQuery = mTree.getNearestByRange(point, r);

                    for (ResultItem<SlicingProudData> resultItem:treeQuery) {
                        point.count_after++;
                        neighbourCount++;
                    }

                    if (point.count_after >= k)
                        point.safe_inlier = true;
                }

                point.last_check = nextSlide;
                nextSlide += slideDuration;
            }


            if (neighbourCount < k) {
                state.triggers.get(-SlicingProudAlgorithmExecutor.OUTLIERS_TRIGGER)
                        .add(point.id);
            }

        }

        private long getSlide(long arrivalTime, long slideDuration) {
            long first = arrivalTime - window.start;
            long div = first / slideDuration;
            return window.start + (div * slideDuration);
        }

        public void insertPoint(SlicingProudData point) {
            long slide = query.s;
            int k = query.k;
            double r = query.r;

            int neighbourCount = 0;
            long nextSlide = window.end - slide;

            while (neighbourCount < k && nextSlide >= window.start) {
                MTree<SlicingProudData> mTree = state.trees.getOrDefault(nextSlide, null);

                if (mTree != null) {
                    boolean hasNeighbours = false;
                    MTree<SlicingProudData>.Query treeQuery = mTree.getNearestByRange(point, r);

                    for (ResultItem<SlicingProudData> resultItem:treeQuery) {
                        SlicingProudData node = resultItem.data;

                        if (!hasNeighbours) {
                            hasNeighbours = true;
                            state.triggers
                                    .get(nextSlide)
                                    .add(point.id);
                        }

                        if (nextSlide == window.end - slide) {
                            if (node.id != point.id) {
                                point.count_after++;
                                neighbourCount++;
                            }
                        } else {
                            point.slices_before.put(nextSlide, point.slices_before.getOrDefault(nextSlide, 0) + 1);
                            neighbourCount++;
                        }
                    }

                    if (nextSlide == window.end - slide && neighbourCount >= k)
                        point.safe_inlier = true;
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