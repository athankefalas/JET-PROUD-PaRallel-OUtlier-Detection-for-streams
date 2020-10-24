package edu.auth.jetproud.proud.algorithms;

import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.datastructures.vptree.distance.DistanceFunction;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.NaiveProudData;
import edu.auth.jetproud.proud.ProudContext;
import edu.auth.jetproud.proud.algorithms.exceptions.UnsupportedSpaceException;
import edu.auth.jetproud.proud.algorithms.functions.ProudComponentBuilder;
import edu.auth.jetproud.proud.statistics.ProudStatistics;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class NaiveProudAlgorithmExecutor extends AnyProudAlgorithmExecutor<NaiveProudData>
{

    public NaiveProudAlgorithmExecutor(ProudContext proudContext) {
        super(proudContext, ProudAlgorithmOption.Naive);
    }

    @Override
    protected <D extends AnyProudData> NaiveProudData transform(D point) {
        return new NaiveProudData(point);
    }

    @Override
    protected Object processSingleSpace(StreamStage<KeyedWindowResult<Integer, List<Tuple<Integer, NaiveProudData>>>> windowedStage) throws UnsupportedSpaceException {
        // TODO impl
        ProudComponentBuilder componentBuilder = ProudComponentBuilder.create(proudContext);

        // Create Outlier Query - Queries
        int w =proudContext.getProudConfiguration().getWindowSizes().get(0);
        int s = proudContext.getProudConfiguration().getSlideSizes().get(0);
        double r = proudContext.getProudConfiguration().getRNeighbourhood().get(0);
        int k = proudContext.getProudConfiguration().getKNeighbours().get(0);

        final OutlierQuery outlierQuery = new OutlierQuery(r,k,w,s);

        final int slide = outlierQuery.s;
        final int K = outlierQuery.k;
        final double R = outlierQuery.r;

        StreamStage<List<NaiveProudData>> detectOutliersStage = windowedStage.rollingAggregate(
                componentBuilder.outlierAggregator((outliers, window)->{
                    // Detect outliers and add them to outliers accumulator
                    int partition = window.getKey();

                    long windowStart = window.start();
                    long windowEnd = window.end();


                    DistanceFunction<NaiveProudData> distance = DistanceFunction.euclidean();
                    List<NaiveProudData> windowItems = window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((it)->it.arrival >= windowEnd - slide)
                            .collect(Collectors.toList());

                    // Traverse all nodes in the window
                    for(NaiveProudData currentNode : windowItems) {

                        // Find current node neighbours
                        List<NaiveProudData> neighbours = windowItems.stream()
                                .filter((it)->it.id != currentNode.id)
                                .map((it)->new Tuple<>(it, distance.getDistance(currentNode, it)))
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

                            if (neighbour.count_after >= k)
                                neighbour.safe_inlier = true;
                        }
                    }

                    // Add all non-safe inliers to the outliers accumulator
                    for (NaiveProudData currentNode : windowItems) {
                        // Move to 2nd loop
                        if (!currentNode.safe_inlier)
                            outliers.add(currentNode);
                    }
                })
        );

        // don't flatten yet !!!

        //detectOutliersStage. same but return StreamStage<List<Tuple<Double,Query>>>

        //flatten here and then to pipeline Sink
        StreamStage<NaiveProudData> temp = detectOutliersStage.flatMap(Traversers::traverseIterable);

        //return final stage
        return super.processSingleSpace(windowedStage);
    }
}
