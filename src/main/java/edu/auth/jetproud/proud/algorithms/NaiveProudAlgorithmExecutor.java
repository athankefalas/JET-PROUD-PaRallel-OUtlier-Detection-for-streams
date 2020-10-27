package edu.auth.jetproud.proud.algorithms;

import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.NaiveProudData;
import edu.auth.jetproud.model.meta.OutlierMetadata;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.ProudContext;
import edu.auth.jetproud.proud.algorithms.exceptions.UnsupportedSpaceException;
import edu.auth.jetproud.proud.algorithms.functions.ProudComponentBuilder;
import edu.auth.jetproud.proud.distributables.DistributedMap;
import edu.auth.jetproud.utils.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class NaiveProudAlgorithmExecutor extends AnyProudAlgorithmExecutor<NaiveProudData>
{
    public static final String METADATA_STATE = "METADATA_STATE";

    public NaiveProudAlgorithmExecutor(ProudContext proudContext) {
        super(proudContext, ProudAlgorithmOption.Naive);
    }

    @Override
    public void createDistributableData() {
        super.createDistributableData();
        DistributedMap<String, OutlierMetadata<NaiveProudData>> state = new DistributedMap<>(METADATA_STATE);
    }

    @Override
    protected <D extends AnyProudData> NaiveProudData transform(D point) {
        return new NaiveProudData(point);
    }

    @Override
    protected Object processSingleSpace(StreamStage<KeyedWindowResult<Integer, List<Tuple<Integer, NaiveProudData>>>> windowedStage) throws UnsupportedSpaceException {
        // Initialize distributed stateful data
        createDistributableData();
        final DistributedMap<String, OutlierMetadata<NaiveProudData>> state = new DistributedMap<>(METADATA_STATE);

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

        StreamStage<List<NaiveProudData>> detectOutliersStage = windowedStage.rollingAggregate(
                components.outlierAggregator((outliers, window)->{
                    // Detect outliers and add them to outliers accumulator
                    int partition = window.getKey();

                    long windowStart = window.start();
                    long windowEnd = window.end();


                    List<NaiveProudData> windowItems = window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((it)->it.arrival >= windowEnd - slide)
                            .collect(Collectors.toList());

                    // Traverse all nodes in the window
                    for(NaiveProudData currentNode : windowItems) {

                        // Find current node neighbours
                        List<NaiveProudData> neighbours = windowItems.stream()
                                .filter((it)->it.id != currentNode.id)
                                .map((it)->new Tuple<>(it, AlgorithmUtils.distanceOf(currentNode, it)))
                                .filter((it)->it.second <= R)
                                .map(Tuple::getFirst)
                                .collect(Collectors.toList());

                        // Update nodes before list and count after counter
                        // for each neighbour
                        for (NaiveProudData neighbour: neighbours) {
                            if (neighbour.arrival < windowEnd - slide) {
                                currentNode.insert_nn_before(neighbour.arrival, k);
                            } else {
                                currentNode.count_after++;

                                if (currentNode.count_after >= k)
                                    currentNode.safe_inlier = true;
                            }
                        }

                        // Find nodes with non expired neighbours
                        List<NaiveProudData> activeNeighbours = windowItems.stream()
                                .filter((it)->it.arrival < windowEnd - slide && neighbours.contains(it))
                                .collect(Collectors.toList());

                        for (NaiveProudData neighbour: activeNeighbours) {
                            neighbour.count_after++;

                            if (neighbour.count_after >= K)
                                neighbour.safe_inlier = true;
                        }
                    }

                    // Add all non-safe inliers to the outliers accumulator
                    for (NaiveProudData currentNode : windowItems) {
                        if (!currentNode.safe_inlier)
                            outliers.add(currentNode);
                    }
                })
        );

        // Group Metadata
        StreamStage<List<Tuple<Long, OutlierQuery>>> outStage =
                detectOutliersStage.flatMap(Traversers::traverseIterable)
                .window(WindowDefinition.tumbling(windowSize))
                .groupingKey((it)->it.id % partitionsCount)
                .aggregate(components.metaWindowAggregator())
                .rollingAggregate(
                        components.metadataAggregator((acc, window)->{
                            int windowKey = window.getKey();

                            long windowStart = window.start();
                            long windowEnd = window.end();

                            List<NaiveProudData> elements = window.getValue();

                            final String METADATA_KEY = "METADATA";
                            OutlierMetadata<NaiveProudData> current = state.get(METADATA_KEY);

                            // Create / Update state Map
                            if(current == null) {
                                Map<Integer,NaiveProudData> outliersMap = new HashMap<>();

                                for (NaiveProudData el:elements) {
                                    NaiveProudData oldElement = outliersMap.getOrDefault(el.id, null);

                                    if (oldElement == null) {
                                        outliersMap.put(el.id, el);
                                    } else {
                                        NaiveProudData combined = AlgorithmUtils.combineElements(oldElement, el, k);
                                        outliersMap.put(el.id, combined);
                                    }

                                }

                                current = new OutlierMetadata<>(outliersMap);
                            } else {

                                // Remove expired elements
                                current.getOutliers().values()
                                        .removeIf((el) -> {
                                            return elements.stream().noneMatch((it) -> el.id == it.id);
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
                                            NaiveProudData combinedValue = AlgorithmUtils.combineElements(oldElement, el, k);
                                            current.getOutliers().put(el.id, combinedValue);
                                        }

                                    }

                                }
                            }

                            // Write state
                            state.put(METADATA_KEY, current);

                            int outliers = 0;

                            for (NaiveProudData el:current.getOutliers().values()) {
                                long nnBefore = el.nn_before.stream()
                                        .filter((it)->it >= windowEnd - w)
                                        .count();

                                if (nnBefore + el.count_after < k)
                                    outliers++;
                            }

                            OutlierQuery queryCopy = outlierQuery.withOutlierCount(outliers);
                            acc.add(new Tuple<>(windowEnd, queryCopy));
                        })
                );

        // TODO: Return the proper stream stage
        //flatten here and then to pipeline Sink

        //return final stage
        return super.processSingleSpace(windowedStage);
    }
}
