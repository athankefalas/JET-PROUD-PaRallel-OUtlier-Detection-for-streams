package edu.auth.jetproud.proud.algorithms.executors;

import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.application.parameters.data.ProudSpaceOption;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.McodProudData;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.algorithms.AnyProudAlgorithmExecutor;
import edu.auth.jetproud.proud.algorithms.Distances;
import edu.auth.jetproud.proud.algorithms.exceptions.UnsupportedSpaceException;
import edu.auth.jetproud.proud.algorithms.functions.ProudComponentBuilder;
import edu.auth.jetproud.proud.distributables.KeyedStateHolder;
import edu.auth.jetproud.utils.EuclideanCoordinateList;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PMCODProudAlgorithmExecutor extends AnyProudAlgorithmExecutor<McodProudData>
{

    public static class MicroCluster implements Serializable
    {
        public CopyOnWriteArrayList<Double> center;
        public CopyOnWriteArrayList<McodProudData> points;

        public MicroCluster() {
            this(Lists.make(), Lists.make());
        }

        public MicroCluster(List<Double> center, List<McodProudData> points) {
            this.center = new CopyOnWriteArrayList<>(center);
            this.points = new CopyOnWriteArrayList<>(points);
        }
    }

    public static class PMCODState implements Serializable
    {
        public AtomicInteger mcCounter = new AtomicInteger(1);
        public ConcurrentHashMap<Integer, McodProudData> pd;
        public ConcurrentHashMap<Integer, MicroCluster> mc;

        public PMCODState() {
            this(new HashMap<>(), new HashMap<>());
        }

        public PMCODState(HashMap<Integer, McodProudData> pd, HashMap<Integer, MicroCluster> mc) {
            this.pd = new ConcurrentHashMap<>(pd);
            this.mc = new ConcurrentHashMap<>(mc);
        }
    }

    public PMCODProudAlgorithmExecutor(ProudContext proudContext) {
        super(proudContext, ProudAlgorithmOption.PMCod);
    }

    @Override
    protected <D extends AnyProudData> McodProudData transform(D point) {
        return new McodProudData(point);
    }

    @Override
    public List<ProudSpaceOption> supportedSpaceOptions() {
        return Lists.of(ProudSpaceOption.Single);
    }

    @Override
    protected StreamStage<Tuple<Long, OutlierQuery>> processSingleSpace(StreamStage<KeyedWindowResult<Integer, List<Tuple<Integer, McodProudData>>>> windowedStage) throws UnsupportedSpaceException {
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

        return windowedStage.flatMapStateful(()-> KeyedStateHolder.<String, PMCODState>create(),
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

                    List<McodProudData> elements = window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((it)->it.arrival >= windowEnd - slide)
                            .collect(Collectors.toList());

                    PMCODState current = stateHolder.get(STATE_KEY);

                    if (current == null) {
                        current = new PMCODState();
                        stateHolder.put(STATE_KEY, current);
                    }

                    final PMCOD pmcod = new PMCOD(current, outlierQuery);

                    // Insert new elements
                    for (McodProudData el: elements) {
                        pmcod.insertPoint(el, true, new ArrayList<>());
                    }

                    // Find outliers
                    long outliersCount = current.pd.values().stream()
                            .filter((p)-> !p.safe_inlier && p.flag == 0)
                            .filter((p)->{
                                return p.count_after + p.nn_before.stream()
                                        .filter((key)-> key >= windowStart)
                                        .count() < k;
                            })
                            .count();

                    OutlierQuery queryCopy = outlierQuery.withOutlierCount(outliersCount);
                    outliers.add(new Tuple<>(windowEnd, queryCopy));

                    //Remove old points
                    Set<Integer> deletedMCs = new HashSet<>();

                    window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((it) -> it.arrival < windowStart + slide)
                            .forEach((el)->{
                                int deletedMC = pmcod.deletePoint(el);

                                if (deletedMC > 0)
                                    deletedMCs.add(deletedMC);
                            });

                    // Delete MCs
                    if (!deletedMCs.isEmpty()) {
                        List<McodProudData> reinsertElements = Lists.make();

                        for (int deletedMC:deletedMCs) {
                            reinsertElements.addAll(current.mc.get(deletedMC).points);
                            current.mc.remove(deletedMC);
                        }

                        List<Integer> reinsertedElementIds = reinsertElements.stream()
                                .map((it)->it.id)
                                .collect(Collectors.toList());

                        for (McodProudData el:reinsertElements) {
                            pmcod.insertPoint(el, false, reinsertedElementIds);
                        }
                    }

                    // If micro-cluster is needed as part of the distributed state remove the following line
                    //current.mcCounter = new AtomicInteger(1);
                    stateHolder.put(STATE_KEY, current);

                    // Metrics & Statistics
                    stopRecordingMetrics(metricsRecorder);

                    // Return results
                    return Traversers.traverseIterable(outliers);
                });
    }

    private static class PMCOD implements Serializable
    {
        public PMCODState state;
        public OutlierQuery outlierQuery;

        public PMCOD(PMCODState state, OutlierQuery outlierQuery) {
            this.state = state;
            this.outlierQuery = outlierQuery;
        }

        public void insertPoint(McodProudData el, boolean newPoint, List<Integer> reinsertIds) {

            if (!newPoint)
                el.clear(-1);

            Map<Integer, Double> closeMicroClusters = findCloseMicroClusters(el);
            Tuple<Integer, Double> closestMC = closeMicroClusters.entrySet().stream()
                    .map(Tuple::fromEntry)
                    .min(Comparator.comparingDouble(Tuple::getSecond))
                    .orElse(new Tuple<>(0, Double.MAX_VALUE));

            if (closestMC.second <= outlierQuery.range / 2.0) {

                if (newPoint) { //Insert element to MC
                    insertToMicroCluster(el, closestMC.first, true, new ArrayList<>());
                } else {
                    insertToMicroCluster(el, closestMC.first, false, reinsertIds);
                }

            } else { //Check against PD
                List<McodProudData> NC = new ArrayList<>();
                List<McodProudData> NNC = new ArrayList<>();

                //
                List<Tuple<Double, McodProudData>> nearItems = state.pd.values().stream()
                        .map(val -> new Tuple<>(Distances.distanceOf(el, val), val))
                        .filter((it) -> it.first <= (3.0 * outlierQuery.range) / 2.0)
                        .collect(Collectors.toList());

                for (Tuple<Double, McodProudData> item: nearItems) {

                    if (item.first <= outlierQuery.range) { // Update metadata
                        addNeighbour(el, item.second);

                        if (newPoint) {
                            addNeighbour(item.second, el);
                        } else {
                            if (reinsertIds.contains(item.second.id)) {
                                addNeighbour(item.second, el);
                            }
                        }
                    }

                    if (item.first <= outlierQuery.range / 2.0)
                        NC.add(item.second);
                    else
                        NNC.add(item.second);
                }


                if (NC.size() >= outlierQuery.kNeighbours) { // Create new MC
                    createMicroCluster(el, NC, NNC);
                } else { //Insert in PD
                    closeMicroClusters.forEach((mc, dist)-> el.Rmc.add(mc));
                    List<Tuple<Integer, PMCODProudAlgorithmExecutor.MicroCluster>> microClusters = state.mc.entrySet().stream()
                            .filter((mc) -> closeMicroClusters.containsKey(mc.getKey()))
                            .map(Tuple::fromEntry)
                            .collect(Collectors.toList());

                    for (Tuple<Integer, PMCODProudAlgorithmExecutor.MicroCluster> currentMicroCluster:microClusters) {
                        for (McodProudData point:currentMicroCluster.second.points) {
                            double distance = Distances.distanceOf(el, point);

                            if (distance <= outlierQuery.range)
                                addNeighbour(el, point);
                        }
                    }

                    state.pd.put(el.id, el);
                }
            }
        }

        public int deletePoint(McodProudData el) {
            int result = 0;

            if (el.mc <= 0) {
                state.pd.remove(el.id);
            } else {
                PMCODProudAlgorithmExecutor.MicroCluster mc = state.mc.get(el.mc);

                if (mc != null) {
                    mc.points.removeIf((it)->it.id == el.id);

                    if (mc.points.size() <= outlierQuery.kNeighbours) {
                        result = el.mc;
                    }
                }
            }

            return result;
        }

        private void createMicroCluster(McodProudData el, List<McodProudData> NC, List<McodProudData> NNC) {
            int mcCounter = state.mcCounter.get();

            for (McodProudData it:NC) {
                it.clear(mcCounter);
                state.pd.remove(it.id);
            }

            el.clear(mcCounter);
            NC.add(el);

            PMCODProudAlgorithmExecutor.MicroCluster newMC = new PMCODProudAlgorithmExecutor.MicroCluster(el.value, NC);
            state.mc.put(mcCounter, newMC);

            for (McodProudData it:NNC) {
                it.Rmc.add(mcCounter);
            }

            mcCounter += 1;
            state.mcCounter.set(mcCounter);
        }

        private void insertToMicroCluster(McodProudData el, int mc, boolean update, List<Integer> reinsertIds) {
            el.clear(mc);
            state.mc.get(mc).points.add(el);

            List<McodProudData> values = state.pd.values().stream()
                    .filter((it)-> it.Rmc.contains(mc) && (update || reinsertIds.contains(it.id)))
                    .collect(Collectors.toList());

            for (McodProudData it: values) {
                if (Distances.distanceOf(it, el) <= outlierQuery.range) {
                    addNeighbour(it, el);
                }
            }
        }

        private void addNeighbour(McodProudData el, McodProudData neighbour) {
            int k = outlierQuery.kNeighbours;

            if (el.arrival > neighbour.arrival) {
                el.insert_nn_before(neighbour.arrival, k);
            } else {
                el.count_after++;

                if (el.count_after >= k)
                    el.safe_inlier = true;
            }
        }

        private Map<Integer,Double> findCloseMicroClusters(McodProudData el) {
            final double R = outlierQuery.range;
            Map<Integer,Double> res = new HashMap<>();

            state.mc.entrySet().stream()
                    .map((entry) -> new Tuple<>(entry.getKey(), Distances.distanceOf(el, new EuclideanCoordinateList<>(entry.getValue().center))))
                    .filter((it)-> it.second <= (3.0 * R) / 2.0)
                    .forEach((it)->res.put(it.first, it.second));

            return res;
        }
    }
}
