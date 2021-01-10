package edu.auth.jetproud.proud.algorithms.executors;

import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.application.parameters.data.ProudSpaceOption;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.NaiveProudData;
import edu.auth.jetproud.model.meta.OutlierMetadata;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.algorithms.Distances;
import edu.auth.jetproud.proud.algorithms.AnyProudAlgorithmExecutor;
import edu.auth.jetproud.proud.algorithms.exceptions.UnsupportedSpaceException;
import edu.auth.jetproud.proud.algorithms.functions.ProudComponentBuilder;
import edu.auth.jetproud.proud.distributables.KeyedStateHolder;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.io.Serializable;
import java.util.List;
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
            // Metrics & Statistics
            SlideMetricsRecorder metricsRecorder = startRecordingMetrics();

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
                naive.updateMetadataOf(currentNode, windowItems);
            }

            // Add all non-safe in-liers to the outliers accumulator
            for (NaiveProudData currentNode : windowItems) {
                if (!currentNode.safe_inlier.get())
                    outliers.add(currentNode.copy()); // @See resources/info/ReferenceIssues
            }

            // Statistics
            stopRecordingMetrics(metricsRecorder);

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

                            final String METADATA_KEY = "METADATA_" + windowKey;
                            OutlierMetadata<NaiveProudData> current = stateHolder.get(METADATA_KEY);

                            // Create / Update state Map
                            if(current == null) {
                                current = new OutlierMetadata<>();

                                for (NaiveProudData el:elements) {
                                    NaiveProudData oldElement = current.getOutliers().getOrDefault(el.id, null);

                                    if (oldElement == null) {
                                        current.getOutliers().put(el.id, el.copy());
                                    } else {
                                        NaiveProudData combined = Naive.combineElements(oldElement.copy(), el.copy(), k);
                                        current.getOutliers().put(el.id, combined);
                                    }

                                }

                            } else {

                                // Remove expired elements and safe in-liers
                                List<Integer> idsToRemove = Lists.make();

                                for (NaiveProudData el: current.getOutliers().values()) {
                                    if (elements.stream().noneMatch((it) -> it.id == el.id)) {
                                        idsToRemove.add(el.id);
                                    }
                                }

                                for (Integer id : idsToRemove) {
                                    current.getOutliers().remove(id);
                                }

                                // Combine remaining elements
                                for(NaiveProudData el: elements) {
                                    NaiveProudData oldElement = current.getOutliers().getOrDefault(el.id, null);

                                    if (oldElement == null) {
                                        current.getOutliers().put(el.id, el.copy());
                                    } else {
                                        if (el.arrival < windowEnd - slide) {
                                            oldElement = oldElement.copy();
                                            oldElement.count_after.set(el.count_after.get());
                                            current.getOutliers().put(el.id, oldElement);
                                        } else {
                                            NaiveProudData combinedValue = Naive.combineElements(oldElement.copy(), el.copy(), k);
                                            current.getOutliers().put(el.id, combinedValue);
                                        }
                                    }

                                }
                            }

                            List<NaiveProudData> outlierValues = Lists.copyOf(current.getOutliers().values());

                            // Write state
                            stateHolder.put(METADATA_KEY, current);

                            int outliers = 0;

                            for (NaiveProudData el:outlierValues) {
                                long neighboursBefore = el.nn_before.stream()
                                        .filter((it)-> it >= windowEnd - w)
                                        .count();

                                if (neighboursBefore + el.count_after.get() < k) {
                                    outliers++;
                                }
                            }

                            OutlierQuery queryCopy = outlierQuery.withOutlierCount(outliers);
                            return Traversers.singleton(new Tuple<>(windowEnd, queryCopy));
                        });
    }



    private static class Naive implements Serializable
    {

        public static List<NaiveProudData> evict(List<NaiveProudData> windowData, long windowStart, long windowEnd, long slide) {
            List<NaiveProudData> evictedWindowData = Lists.copyOf(windowData);

            evictedWindowData.removeIf((it)-> {
                return it.flag == 1 && it.arrival >= windowStart && it.arrival < windowEnd - slide;
            });

            return evictedWindowData;
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

        public void updateMetadataOf(NaiveProudData node, List<NaiveProudData> nodes) {
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
                    node.count_after.addAndGet(1);

                    if (node.count_after.get() >= K) {
                        node.safe_inlier.set(true);
                    }
                }
            }

            List<NaiveProudData> inactive = nodes.stream()
                    .filter((it) -> it.arrival < windowEnd - slide && neighbours.stream().anyMatch((n)->n.id == it.id))
                    .collect(Collectors.toList());

            // Add new neighbor to previous nodes
            for (NaiveProudData neighbour: inactive) {
                neighbour.count_after.addAndGet(1);

                if (neighbour.count_after.get() >= K)
                    neighbour.safe_inlier.set(true);
            }
        }

        public static NaiveProudData combineElements(NaiveProudData oldItem, NaiveProudData newItem, int k) {

            for(Long elementTimestamp: newItem.nn_before) {
                oldItem.insert_nn_before(elementTimestamp, k);
            }

            return oldItem;
        }

    }

}
