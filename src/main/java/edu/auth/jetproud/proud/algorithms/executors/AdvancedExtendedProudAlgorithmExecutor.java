package edu.auth.jetproud.proud.algorithms.executors;

import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.application.parameters.data.ProudSpaceOption;
import edu.auth.jetproud.datastructures.mtree.MTree;
import edu.auth.jetproud.datastructures.mtree.ResultItem;
import edu.auth.jetproud.datastructures.mtree.distance.DistanceFunction;
import edu.auth.jetproud.datastructures.mtree.partition.PartitionFunction;
import edu.auth.jetproud.datastructures.mtree.promotion.PromotionFunction;
import edu.auth.jetproud.datastructures.mtree.split.SplitFunction;
import edu.auth.jetproud.model.AdvancedProudData;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.algorithms.AnyProudAlgorithmExecutor;
import edu.auth.jetproud.proud.algorithms.exceptions.UnsupportedSpaceException;
import edu.auth.jetproud.proud.algorithms.functions.ProudComponentBuilder;
import edu.auth.jetproud.proud.distributables.DistributedMap;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class AdvancedExtendedProudAlgorithmExecutor extends AnyProudAlgorithmExecutor<AdvancedProudData>
{
    public static final String DATA_STATE = "ADVANCED_EXT_DATA_STATE";

    public static class AdvancedExtendedState {
        public MTree<AdvancedProudData> mTree;
        public HashMap<Integer, AdvancedProudData> map;

        public AdvancedExtendedState() {
        }

        public AdvancedExtendedState(MTree<AdvancedProudData> mTree, HashMap<Integer, AdvancedProudData> map) {
            this.mTree = mTree;
            this.map = map;
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
    public void createDistributableData() {
        super.createDistributableData();
        DistributedMap<String, AdvancedExtendedState> stateMap = new DistributedMap<>(DATA_STATE);
    }

    @Override
    public List<ProudSpaceOption> supportedSpaceOptions() {
        return Lists.of(ProudSpaceOption.Single);
    }

    @Override
    protected StreamStage<Tuple<Long, OutlierQuery>> processSingleSpace(StreamStage<KeyedWindowResult<Integer, List<Tuple<Integer, AdvancedProudData>>>> windowedStage) throws UnsupportedSpaceException {
        // Initialize distributed stateful data
        createDistributableData();
        final DistributedMap<String, AdvancedExtendedState> stateMap = new DistributedMap<>(DATA_STATE);

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

        StreamStage<List<Tuple<Long, OutlierQuery>>> detectOutliersStage = windowedStage.rollingAggregate(
                components.outlierDetection((outliers, window)->{
                    // Detect outliers and add them to outliers accumulator
                    int partition = window.getKey();

                    long windowStart = window.start();
                    long windowEnd = window.end();

                    final String STATE_KEY = "STATE";

                    AdvancedExtendedState current = stateMap.getOrDefault(STATE_KEY, null);

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
                            mTree.add(el);
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
                            current.mTree.add(el);
                            current.map.put(el.id, el);
                        }
                    }

                    for (AdvancedProudData el: elements) {
                        MTree<AdvancedProudData>.Query treeQuery = current.mTree.getNearestByRange(el, r);

                        for (ResultItem<AdvancedProudData> item:treeQuery) {
                            AdvancedProudData node = item.data;

                            if (node.id == el.id)
                                continue;

                            if (node.arrival < windowEnd - slide) {
                                //
                                AdvancedProudData element = current.map.get(el.id);
                                AdvancedProudData neighbour = current.map.get(node.id);

                                if (element.flag == 0)
                                    element.insert_nn_before(neighbour.arrival, k);

                                if (neighbour.flag == 0) {
                                    neighbour.count_after++;
                                    if (neighbour.count_after >= k)
                                        neighbour.safe_inlier = true;
                                }

                            } else {
                                AdvancedProudData element = current.map.get(el.id);

                                if (el.flag == 0) {
                                    element.count_after++;

                                    if (element.count_after >= k)
                                        element.safe_inlier = true;
                                }
                            }
                        }
                    }

                    // Add outliers to accumulator

                    int outliersCount = 0;

                    for (AdvancedProudData el:current.map.values()) {
                        if (el.flag == 0 && !el.safe_inlier) {
                            long nnBefore = el.nn_before.stream()
                                    .filter((it)->it >= windowEnd - w)
                                    .count();

                            if (el.count_after + nnBefore < k)
                                outliersCount++;
                        }
                    }

                    OutlierQuery queryCopy = outlierQuery.withOutlierCount(outliersCount);
                    outliers.add(new Tuple<>(windowEnd, queryCopy));

                    // Remove expiring and flagged objects from MTree
                    List<AdvancedProudData> toRemove = elements.stream()
                            .filter((el) -> el.arrival < windowStart + slide)
                            .collect(Collectors.toList());

                    for (AdvancedProudData item:toRemove) {
                        current.mTree.remove(item);
                        current.map.remove(item.id);
                    }

                    // Update state
                    stateMap.put(STATE_KEY, current);

                })
        );

        // Return flattened stream
        StreamStage<Tuple<Long, OutlierQuery>> flattenedResult = detectOutliersStage.flatMap(Traversers::traverseIterable);
        return flattenedResult;
    }



}
