package edu.auth.jetproud.proud.algorithms.executors;

import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.application.config.ProudConfiguration;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.application.parameters.data.ProudSpaceOption;
import edu.auth.jetproud.model.AmcodProudData;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.algorithms.AnyProudAlgorithmExecutor;
import edu.auth.jetproud.proud.algorithms.Distances;
import edu.auth.jetproud.proud.algorithms.exceptions.UnsupportedSpaceException;
import edu.auth.jetproud.proud.algorithms.functions.ProudComponentBuilder;
import edu.auth.jetproud.proud.distributables.DistributedMap;
import edu.auth.jetproud.proud.distributables.KeyedStateHolder;
import edu.auth.jetproud.utils.ArrayUtils;
import edu.auth.jetproud.utils.EuclideanCoordinateList;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class AMCODProudAlgorithmExecutor extends AnyProudAlgorithmExecutor<AmcodProudData>
{

    public static class AMCODMicroCluster implements Serializable
    {
        public CopyOnWriteArrayList<Double> center;
        public CopyOnWriteArrayList<AmcodProudData> points;

        public AMCODMicroCluster() {
            this(new LinkedList<>(),new LinkedList<>());
        }

        public AMCODMicroCluster(List<Double> center, List<AmcodProudData> points) {
            this.center = new CopyOnWriteArrayList<>(center);
            this.points = new CopyOnWriteArrayList<>(points);
        }
    }

    public static class AMCODState implements Serializable
    {
        public AtomicInteger mcCounter = new AtomicInteger(1);
        public ConcurrentHashMap<Integer, AmcodProudData> pd;
        public ConcurrentHashMap<Integer, AMCODMicroCluster> mc;

        public AMCODState() {
            this(new HashMap<>(), new HashMap<>());
        }

        public AMCODState(HashMap<Integer, AmcodProudData> pd, HashMap<Integer, AMCODMicroCluster> mc) {
            this.pd = new ConcurrentHashMap<>(pd);
            this.mc = new ConcurrentHashMap<>(mc);
        }
    }

    public AMCODProudAlgorithmExecutor(ProudContext proudContext) {
        super(proudContext, ProudAlgorithmOption.AMCod);
    }

    @Override
    protected <D extends AnyProudData> AmcodProudData transform(D point) {
        return new AmcodProudData(point);
    }

    @Override
    public List<ProudSpaceOption> supportedSpaceOptions() {
        return Lists.of(ProudSpaceOption.Single);
    }

    @Override
    protected StreamStage<Tuple<Long, OutlierQuery>> processMultiQueryParamsSpace(StreamStage<KeyedWindowResult<Integer, List<Tuple<Integer, AmcodProudData>>>> windowedStage) throws UnsupportedSpaceException {
        final long windowSize = proudContext.internalConfiguration().getCommonW();
        final int partitionsCount = proudContext.internalConfiguration().getPartitions();
        ProudComponentBuilder components = ProudComponentBuilder.create(proudContext);

        // Create Outlier Query - Queries

        final ProudConfiguration proudConfig = proudContext.configuration();
        final List<OutlierQuery> outlierQueries = Lists.make();

        for (int w : proudConfig.getWindowSizes()) {
            for (int s : proudConfig.getSlideSizes()) {
                for (double r : proudConfig.getRNeighbourhood()) {
                    for (int k : proudConfig.getKNeighbours()) {
                        outlierQueries.add(new OutlierQuery(r, k, w, s));
                    }
                }
            }
        }

        final int slide = outlierQueries.get(0).slide;

        List<Double> R_distinct_list = outlierQueries.stream()
                .map(OutlierQuery::getRange)
                .distinct()
                .sorted()
                .collect(Collectors.toList());

        List<Integer> k_distinct_list = outlierQueries.stream()
                .map(OutlierQuery::getKNeighbours)
                .distinct()
                .sorted()
                .collect(Collectors.toList());

        final double R_max = R_distinct_list.stream()
                .max(Comparator.naturalOrder())
                .orElse(0.0);
        final double R_min = R_distinct_list.stream()
                .min(Comparator.naturalOrder())
                .orElse(0.0);
        final int k_max = k_distinct_list.stream()
                .max(Comparator.naturalOrder())
                .orElse(0);
        final int k_min = k_distinct_list.stream()
                .min(Comparator.naturalOrder())
                .orElse(0);

        final int k_size = k_distinct_list.size();
        final int R_size = R_distinct_list.size();

        return windowedStage.flatMapStateful(()->KeyedStateHolder.<String, AMCODState>create(),
                (stateHolder, window) -> {
                    // Detect outliers and add them to outliers accumulator
                    List<Tuple<Long, OutlierQuery>> outliers = Lists.make();

                    int partition = window.getKey();

                    long windowStart = window.start();
                    long windowEnd = window.end();

                    final String STATE_KEY = "STATE_"+partition;

                    List<AmcodProudData> elements = window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((it)->it.arrival >= windowEnd - slide)
                            .collect(Collectors.toList());

                    AMCODState current = stateHolder.get(STATE_KEY);

                    if (current == null) {
                        current = new AMCODState();
                        stateHolder.put(STATE_KEY, current);
                    }

                    final AMCOD amcod = new AMCOD(current, R_min, R_max, k_min, k_max);

                    int[][] allQueries = ArrayUtils.multidimensionalWith(0, R_size, k_size);

                    // Insert new elements
                    for (AmcodProudData el: elements) {
                        amcod.insertPoint(el, true, new ArrayList<>());
                    }

                    //Find outliers
                    for(AmcodProudData p:current.pd.values()) {
                        if (!p.safe_inlier && p.flag == 0) {
                            if (p.count_after >= k_max) {
                                p.nn_before_set.clear();
                                p.safe_inlier = true;
                            } else {
                                int i = 0;
                                int y = 0;

                                long beforeCount = p.nn_before_set.stream()
                                        .filter((it)->it.first >= windowStart && it.second <= R_distinct_list.get(0))
                                        .count();

                                long afterCount = p.count_after_set.stream()
                                        .filter((it)-> it <= R_distinct_list.get(0))
                                        .count();

                                long count = beforeCount + afterCount;

                                do {
                                    if (count >= k_distinct_list.get(y)) { //inlier for all i
                                        y += 1;
                                    } else { //outlier for all y
                                        for (int z=y;z<k_size;z++) {
                                            allQueries[i][z] += 1;
                                        }

                                        i += 1;

                                        if (i < R_size) {
                                            final int index = i;

                                            beforeCount = p.nn_before_set.stream()
                                                    .filter((it)->it.first >= windowStart && it.second <= R_distinct_list.get(index))
                                                    .count();

                                            afterCount = p.count_after_set.stream()
                                                    .filter((it)-> it <= R_distinct_list.get(index))
                                                    .count();

                                            count = beforeCount + afterCount;
                                        }
                                    }
                                } while (i < R_size && y < k_size);
                            }
                        }
                    }

                    // Publish outliers to accumulator
                    for (int i=0;i<R_size;i++) {
                        for (int y=0;y<k_size;y++) {
                            OutlierQuery outlierQuery = new OutlierQuery(
                                    R_distinct_list.get(i),
                                    k_distinct_list.get(y),
                                    outlierQueries.get(0).window,
                                    outlierQueries.get(0).slide
                            ).withOutlierCount(allQueries[i][y]);

                            outliers.add(new Tuple<>(windowEnd, outlierQuery));
                        }
                    }

                    //Remove old points
                    Set<Integer> deletedMCs = new HashSet<>();

                    window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((it) -> it.arrival < windowStart + slide)
                            .forEach((el)->{
                                int deletedMC = amcod.deletePoint(el);

                                if (deletedMC > 0)
                                    deletedMCs.add(deletedMC);
                            });

                    // Delete MCs
                    if (!deletedMCs.isEmpty()) {
                        List<AmcodProudData> reinsertElements = Lists.make();

                        for (int deletedMC:deletedMCs) {
                            reinsertElements.addAll(current.mc.get(deletedMC).points);
                            current.mc.remove(deletedMC);
                        }

                        List<Integer> reinsertedElementIds = reinsertElements.stream()
                                .map((it)->it.id)
                                .sorted()
                                .collect(Collectors.toList());

                        for (AmcodProudData el:reinsertElements) {
                            amcod.insertPoint(el, false, reinsertedElementIds);
                        }
                    }

                    // Update state
                    stateHolder.put(STATE_KEY, current);

                    // Return results
                    return Traversers.traverseIterable(outliers);
                });
    }

    private static class AMCOD implements Serializable
    {
        public AMCODState state;

        public double R_min;
        public double R_max;
        public int K_min;
        public int K_max;

        public AMCOD(AMCODState state, double r_min, double r_max, int k_min, int k_max) {
            this.state = state;
            R_min = r_min;
            R_max = r_max;
            K_min = k_min;
            K_max = k_max;
        }

        public void insertPoint(AmcodProudData el, boolean newPoint, List<Integer> reinsertIds) {

            if (!newPoint)
                el.clear(-1);

            Map<Integer, Double> closeMicroClusters = findCloseMicroClusters(el);
            Tuple<Integer, Double> closestMC = closeMicroClusters.entrySet().stream()
                    .map(Tuple::fromEntry)
                    .min(Comparator.comparingDouble(Tuple::getSecond))
                    .orElse(new Tuple<>(0, Double.MAX_VALUE));

            if (closestMC.second < R_min / 2.0) {

                if (newPoint) { //Insert element to MC
                    insertToMicroCluster(el, closestMC.first, true, new ArrayList<>());
                } else {
                    insertToMicroCluster(el, closestMC.first, false, reinsertIds);
                }

            } else { //Check against PD
                List<AmcodProudData> NC = new ArrayList<>();
                List<AmcodProudData> NNC = new ArrayList<>();

                //
                List<Tuple<Double, AmcodProudData>> nearItems = state.pd.values().stream()
                        .map(val -> new Tuple<>(Distances.distanceOf(el, val), val))
                        .filter((it) -> it.first <= 3 * (R_max / 2.0))
                        .collect(Collectors.toList());

                for (Tuple<Double, AmcodProudData> item: nearItems) {

                    if (item.first <= R_max) { // Update metadata
                        addNeighbour(el, item.second, item.first);

                        if (newPoint) {
                            addNeighbour(item.second, el, item.first);
                        } else {
                            if (reinsertIds.contains(item.second.id)) {
                                addNeighbour(item.second, el, item.first);
                            }
                        }
                    }

                    if (item.first <= R_min / 2.0)
                        NC.add(item.second);
                    else
                        NNC.add(item.second);
                }


                if (NC.size() >= K_max) { // Create new MC
                    createMicroCluster(el, NC, NNC);
                } else { //Insert in PD
                    closeMicroClusters.forEach((mc, dist)-> el.Rmc.add(mc));
                    List<Tuple<Integer, AMCODMicroCluster>> microClusters = state.mc.entrySet().stream()
                            .filter((mc) -> closeMicroClusters.containsKey(mc.getKey()))
                            .map(Tuple::fromEntry)
                            .collect(Collectors.toList());

                    for (Tuple<Integer, AMCODMicroCluster> currentMicroCluster:microClusters) {
                        for (AmcodProudData point:currentMicroCluster.second.points) {
                            double distance = Distances.distanceOf(el, point);

                            if (distance <= R_max)
                                addNeighbour(el, point, distance);
                        }
                    }

                    //Do the skyband
                    List<Tuple<Long, Double>> tmp_nn_before = kSkyband(K_max - el.count_after - 1, el.nn_before_set);
                    el.nn_before_set.clear();
                    el.nn_before_set = new CopyOnWriteArrayList<>();
                    el.nn_before_set.addAll(tmp_nn_before);

                    state.pd.put(el.id, el);
                }
            }
        }

        public int deletePoint(AmcodProudData el) {
            int result = 0;

            if (el.mc <= 0) {
                state.pd.remove(el.id);
            } else {
                AMCODMicroCluster mc = state.mc.get(el.mc);

                if (mc != null) {
                    mc.points.removeIf((it)->it.id == el.id);

                    if (mc.points.size() <= K_max) {
                        result = el.mc;
                    }
                }
            }

            return result;
        }

        private void createMicroCluster(AmcodProudData el, List<AmcodProudData> NC, List<AmcodProudData> NNC) {
            int mcCounter = state.mcCounter.get();

            for (AmcodProudData it:NC) {
                it.clear(mcCounter);
                state.pd.remove(it.id);
            }

            el.clear(mcCounter);
            NC.add(el);

            AMCODMicroCluster newMC = new AMCODMicroCluster(el.value, NC);
            state.mc.put(mcCounter, newMC);

            for (AmcodProudData it:NNC) {
                it.Rmc.add(mcCounter);
            }

            mcCounter += 1;
            state.mcCounter.set(mcCounter);
        }

        private void insertToMicroCluster(AmcodProudData el, int mc, boolean update, List<Integer> reinsertIds) {
            el.clear(mc);

            state.mc.get(mc).points.add(el);

            List<AmcodProudData> values = state.pd.values().stream()
                    .filter((it)-> it.Rmc.contains(mc) && (update || reinsertIds.contains(it.id)))
                    .collect(Collectors.toList());

            for (AmcodProudData it: values) {
                double distance = Distances.distanceOf(it, el);
                if (distance <= R_max) {
                    addNeighbour(it, el, distance);
                }
            }
        }

        private Map<Integer,Double> findCloseMicroClusters(AmcodProudData el) {
            Map<Integer,Double> res = new HashMap<>();

            state.mc.entrySet().stream()
                    .map((entry) -> new Tuple<>(entry.getKey(), Distances.distanceOf(el, new EuclideanCoordinateList<>(entry.getValue().center))))
                    .filter((it)-> it.second <= (3 * R_max) / 2)
                    .forEach((it)->res.put(it.first, it.second));

            return res;
        }

        private void addNeighbour(AmcodProudData el, AmcodProudData neigh, double distance) {
            if (el.arrival > neigh.arrival) {
                el.nn_before_set.add(new Tuple<>(neigh.arrival,distance));
            } else {
                el.count_after_set.add(distance);

                if (distance <= R_min) {
                    el.count_after += 1;
                }
            }
        }

        private List<Tuple<Long,Double>> kSkyband(int k, List<Tuple<Long,Double>> neighboursC) {
            //neighbors should be in ascending order of distances
            List<Tuple<Long,Double>> neighbours = neighboursC.stream()
                    .sorted(Comparator.comparingDouble(Tuple::getSecond))
                    .collect(Collectors.toList());

            List<Tuple<Long,Double>> res = new ArrayList<>();

            for (int i=0; i < neighbours.size(); i++) {
                int counter = 0;

                for (int y=0; y < i; y++) {
                    if (neighbours.get(y).first > neighbours.get(i).first)
                        counter++;
                }

                if (counter <= k)
                    res.add(neighbours.get(i));
            }

            return res;
        }

    }

}
