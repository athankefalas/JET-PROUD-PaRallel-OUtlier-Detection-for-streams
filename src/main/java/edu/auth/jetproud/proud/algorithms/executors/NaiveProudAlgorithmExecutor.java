package edu.auth.jetproud.proud.algorithms.executors;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.application.parameters.data.ProudSpaceOption;
import edu.auth.jetproud.model.AdvancedProudData;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.NaiveProudData;
import edu.auth.jetproud.model.meta.OutlierMetadata;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.algorithms.Distances;
import edu.auth.jetproud.proud.algorithms.AnyProudAlgorithmExecutor;
import edu.auth.jetproud.proud.algorithms.exceptions.UnsupportedSpaceException;
import edu.auth.jetproud.proud.algorithms.functions.ProudComponentBuilder;
import edu.auth.jetproud.proud.distributables.DistributedMap;
import edu.auth.jetproud.proud.distributables.KeyedStateHolder;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class NaiveProudAlgorithmExecutor extends AnyProudAlgorithmExecutor<NaiveProudData>
{
    public NaiveProudAlgorithmExecutor(ProudContext proudContext) {
        super(proudContext, ProudAlgorithmOption.Naive);
    }

    @Override
    public List<ProudSpaceOption> supportedSpaceOptions() {
        return Lists.of(ProudSpaceOption.Single);
    }

    @Override
    protected <D extends AnyProudData> NaiveProudData transform(D point) {
        return new NaiveProudData(point);
    }

    @Override
    protected StreamStage<Tuple<Long, OutlierQuery>> processSingleSpace(StreamStage<KeyedWindowResult<Integer, List<Tuple<Integer, NaiveProudData>>>> windowedStage) throws UnsupportedSpaceException {
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

        StreamStage<NaiveProudData> detectedOutliersStage = windowedStage.flatMap((window)->{
            // Detect outliers and add them to outliers accumulator
            List<NaiveProudData> outliers = Lists.make();
            int partition = window.getKey();

            long windowStart = window.start();
            long windowEnd = window.end();

            List<NaiveProudData> windowItems = window.getValue().stream()
                    .map(Tuple::getSecond)
                    .collect(Collectors.toList());

            // Evict old elements
            windowItems = Naive.evict(windowItems, windowStart, windowEnd, slide);

            Naive naive = new Naive(windowStart, windowEnd, K, R, slide);

            List<NaiveProudData> elements = windowItems.stream()
                    .filter((it)->it.arrival >= windowEnd - slide)
                    .collect(Collectors.toList());

            // Traverse all nodes in the window
            for(NaiveProudData currentNode : elements) {
                naive.refreshList(currentNode, windowItems);
            }

            // Add all non-safe in-liers to the outliers accumulator
            for (NaiveProudData currentNode : windowItems) {
                if (!currentNode.safe_inlier)
                    outliers.add(currentNode);
            }

            return Traversers.traverseIterable(outliers);
        });


        // Group Metadata
        return detectedOutliersStage
                .window(WindowDefinition.tumbling(slideSize))
                .groupingKey((it)->it.id % partitionsCount)
                .aggregate(components.metaWindowAggregator())
                .flatMapStateful(()->KeyedStateHolder.<String, OutlierMetadata<NaiveProudData>>create(),
                        (stateHolder, window) -> {
                            int windowKey = window.getKey();

                            long windowStart = window.start();
                            long windowEnd = window.end();

                            List<NaiveProudData> elements = window.getValue();

                            final String METADATA_KEY = "METADATA_"+windowKey;
                            OutlierMetadata<NaiveProudData> current = stateHolder.get(METADATA_KEY);

                            // Create / Update state Map
                            if(current == null) {
                                HashMap<Integer,NaiveProudData> outliersMap = new HashMap<>();

                                for (NaiveProudData el:elements) {
                                    NaiveProudData oldElement = outliersMap.getOrDefault(el.id, null);

                                    if (oldElement == null) {
                                        outliersMap.put(el.id, el);
                                    } else {
                                        NaiveProudData combined = Naive.combineElements(oldElement, el, k);
                                        outliersMap.put(el.id, combined);
                                    }

                                }

                                current = new OutlierMetadata<>(outliersMap);
                            } else {

                                // Remove expired elements
                                current.getOutliers().values()
                                        .removeIf((el) -> {
                                            return elements.stream().noneMatch((it) -> it.id == el.id);
                                        });

                                // Combine remaining elements
                                for(NaiveProudData el: elements) {
                                    NaiveProudData oldElement = current.getOutliers().getOrDefault(el.id, null);

                                    if (oldElement == null) {
                                        current.getOutliers().put(el.id, el);
                                    } else {

                                        if (el.arrival < windowEnd - slide) {
                                            oldElement.count_after = el.count_after;
                                            current.getOutliers().put(el.id, oldElement);
                                        } else {
                                            NaiveProudData combinedValue = Naive.combineElements(oldElement, el, k);
                                            current.getOutliers().put(el.id, combinedValue);
                                        }

                                    }

                                }
                            }

                            // Write state
                            stateHolder.put(METADATA_KEY, current);

                            int outliers = 0;

                            for (NaiveProudData el:current.getOutliers().values()) {
                                long nnBefore = el.nn_before.stream()
                                        .filter((it)->it >= windowEnd - w)
                                        .count();

                                if (nnBefore + el.count_after < k)
                                    outliers++;
                            }

                            OutlierQuery queryCopy = outlierQuery.withOutlierCount(outliers);
                            return Traversers.singleton(new Tuple<>(windowEnd, queryCopy));
                        });
    }



    private static class Naive implements Serializable
    {

        public static List<NaiveProudData> evict(List<NaiveProudData> windowData, long windowStart, long windowEnd, long slide) {
            List<NaiveProudData> evicted = Lists.copyOf(windowData);

            evicted.removeIf((it)->{
                return it.flag == 1 && it.arrival >= windowStart && it.arrival < windowEnd - slide;
            });

            return evicted;
        }

        long windowStart;
        long windowEnd;

        int K;
        double R;
        int slide;

        public Naive(long windowStart, long windowEnd, int k, double r, int slide) {
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            K = k;
            R = r;
            this.slide = slide;
        }

        public void refreshList(NaiveProudData node, List<NaiveProudData> nodes) {
            if (nodes.isEmpty())
                return;

            List<NaiveProudData> neighbours = nodes.stream()
                    .filter((it) -> it.id != node.id)
                    .filter((it) -> Distances.distanceOf(it, node) <= R)
                    .collect(Collectors.toList());

            for (NaiveProudData neighbour: neighbours) {
                if (neighbour.arrival < windowEnd - slide) {
                    node.insert_nn_before(neighbour.arrival, K);
                } else {
                    node.count_after += 1;

                    if (node.count_after >= K) {
                        node.safe_inlier = true;
                    }
                }
            }

            List<NaiveProudData> active = nodes.stream()
                    .filter((it) -> it.arrival < windowEnd - slide && neighbours.stream().anyMatch((n)->n.id == it.id))
                    .collect(Collectors.toList());

            // Add new neighbor to previous nodes
            for (NaiveProudData neighbour: active) {
                neighbour.count_after += 1;

                if (neighbour.count_after >= K)
                    neighbour.safe_inlier = true;
            }
        }

        public static NaiveProudData combineElements(NaiveProudData one, NaiveProudData other, int k) {
            other.nn_before.forEach((it)->one.insert_nn_before(it, k));
            return one;
        }

    }

}
