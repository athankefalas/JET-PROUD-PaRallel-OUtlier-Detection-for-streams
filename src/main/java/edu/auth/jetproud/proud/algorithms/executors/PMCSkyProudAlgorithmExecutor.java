package edu.auth.jetproud.proud.algorithms.executors;

import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.application.config.ProudConfiguration;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.application.parameters.data.ProudSpaceOption;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.McskyProudData;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.algorithms.AnyProudAlgorithmExecutor;
import edu.auth.jetproud.proud.algorithms.Distances;
import edu.auth.jetproud.proud.algorithms.KeyedWindow;
import edu.auth.jetproud.proud.algorithms.exceptions.UnsupportedSpaceException;
import edu.auth.jetproud.proud.algorithms.functions.ProudComponentBuilder;
import edu.auth.jetproud.proud.distributables.DistributedMap;
import edu.auth.jetproud.utils.ArrayUtils;
import edu.auth.jetproud.utils.EuclideanCoordinateList;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PMCSkyProudAlgorithmExecutor extends AnyProudAlgorithmExecutor<McskyProudData>
{
    public static final String STATES_KEY = "PMCSKY_STATES_KEY";

    public static class PMCSkyState implements Serializable
    {
        public AtomicInteger mcCounter = new AtomicInteger(1);
        public HashMap<Integer, McskyProudData> index;
        public HashMap<Integer, PMCSkyCluster> mc;
        public HashSet<Integer> pd;

        public long slideCount;

        public PMCSkyState() {
            this(new HashMap<>(), new HashMap<>(), new HashSet<>(), 1);
        }

        public PMCSkyState(HashMap<Integer, McskyProudData> index, HashMap<Integer, PMCSkyCluster> mc, HashSet<Integer> pd) {
            this.index = index;
            this.mc = mc;
            this.pd = pd;
        }

        public PMCSkyState(HashMap<Integer, McskyProudData> index, HashMap<Integer, PMCSkyCluster> mc, HashSet<Integer> pd, long slideCount) {
            this.index = index;
            this.mc = mc;
            this.pd = pd;
            this.slideCount = slideCount;
        }
    }

    public static class PMCSkyCluster implements Serializable
    {
        public LinkedList<Double> center;
        public LinkedList<Integer> points;

        public PMCSkyCluster(List<Double> center) {
            this(center, Lists.make());
        }

        public PMCSkyCluster(List<Double> center, List<Integer> points) {
            this.center = new LinkedList<>(center);
            this.points = new LinkedList<>(points);
        }
    }

    public PMCSkyProudAlgorithmExecutor(ProudContext proudContext) {
        super(proudContext, ProudAlgorithmOption.PMCSky);
    }

    @Override
    protected <D extends AnyProudData> McskyProudData transform(D point) {
        return new McskyProudData(point);
    }

    @Override
    public void createDistributableData() {
        super.createDistributableData();
        DistributedMap<String, PMCSkyState> stateMap = new DistributedMap<>(STATES_KEY);
    }

    @Override
    public List<ProudSpaceOption> supportedSpaceOptions() {
        return Lists.of(
                ProudSpaceOption.MultiQueryMultiParams,
                ProudSpaceOption.MultiQueryMultiParamsMultiWindowParams
        );
    }

    @Override
    protected StreamStage<Tuple<Long, OutlierQuery>> processMultiQueryParamsSpace(StreamStage<KeyedWindowResult<Integer, List<Tuple<Integer, McskyProudData>>>> windowedStage) throws UnsupportedSpaceException {
        createDistributableData();
        final DistributedMap<String, PMCSkyState> stateMap = new DistributedMap<>(STATES_KEY);

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

        StreamStage<AppendableTraverser<Tuple<Long,OutlierQuery>>> detectOutliersStage = windowedStage.rollingAggregate(
                components.outlierDetection((outliers, window)->{
                    // Detect outliers and add them to outliers accumulator
                    int partition = window.getKey();

                    long windowStart = window.start();
                    long windowEnd = window.end();
                    long latestSlide = windowEnd - slide;

                    final String STATE_KEY = "STATE";

                    List<McskyProudData> elements = window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((it)->it.arrival >= windowEnd - slide)
                            .collect(Collectors.toList());

                    PMCSkyState current = stateMap.get(STATE_KEY);

                    if (current == null) {
                        current = new PMCSkyState();
                        stateMap.put(STATE_KEY, current);
                    }

                    final KeyedWindow<McskyProudData> windowRef = new KeyedWindow<>(partition, windowStart, windowEnd);
                    final RK_PMCSky pmcsky = new RK_PMCSky(current, windowRef, R_distinct_list, slide, R_min, R_max, k_max);

                    int[][] allQueries = ArrayUtils.multidimensionalWith(0, R_size, k_size);

                    // Insert new elements to state
                    List<McskyProudData> elementsToAdd = elements.stream()
                            //Sort is needed when each point has a different timestamp
                            //In our case every point in the same slide has the same timestamp
                            .sorted(Comparator.comparingLong((it)->it.arrival))
                            .collect(Collectors.toList());

                    for (McskyProudData element:elementsToAdd) {
                        current.index.put(element.id, element);
                    }

                    // Update elements
                    window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((it)->!it.safe_inlier && it.flag == 0 && it.mc == -1)
                            .forEach((p)-> {
                                pmcsky.checkPoint(p);

                                if (p.mc == -1) {
                                    boolean isSafeInlier = p.lsky.getOrDefault(1, Lists.make()).stream()
                                            .filter((it)->it.second >= p.arrival)
                                            .count() >= k_max;

                                    if (isSafeInlier) {
                                        p.safe_inlier = true;
                                    } else {
                                        int i = 0;
                                        int y = 0;

                                        long count = p.lsky.getOrDefault(i+1, Lists.make()).stream()
                                                .filter((it)->it.second > windowStart)
                                                .count();

                                        do {
                                            if(count >= k_distinct_list.get(y)){ // Inlier for all i
                                                y += 1;
                                            } else {  // Outlier for all y
                                                for(int z=y; z < k_size; z++){
                                                    allQueries[i][z] += 1;
                                                }

                                                i += 1;
                                                count += p.lsky.getOrDefault(i+1, Lists.make()).stream()
                                                        .filter((it)->it.second > windowStart)
                                                        .count();
                                            }

                                        }while (i < R_size && y < k_size);
                                    }
                                }
                            });

                    // Report outliers
                    for (int i=0; i < R_size; i++){
                        for (int y=0; y < k_size; y++){
                            OutlierQuery outlierQuery = new OutlierQuery(R_distinct_list.get(i), k_distinct_list.get(y),
                                    outlierQueries.get(0).window,
                                    outlierQueries.get(0).slide
                            ).withOutlierCount(allQueries[i][y]);
                            outliers.append(new Tuple<>(windowEnd, outlierQuery));
                        }
                    }

                    // Remove old points
                    window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((it)-> it.arrival < windowStart + slide)
                            .forEach(pmcsky::deletePoint);

                    // If micro-cluster is needed as part of the distributed state remove the following line
                    current.mcCounter = new AtomicInteger(1);
                    stateMap.put(STATE_KEY, current);
                })
        );

        // Return flattened stream
        StreamStage<Tuple<Long, OutlierQuery>> flattenedResult = detectOutliersStage.flatMap((it)->it);
        return flattenedResult;
    }

    @Override
    protected StreamStage<Tuple<Long, OutlierQuery>> processMultiQueryParamsMultiWindowParamsSpace(StreamStage<KeyedWindowResult<Integer, List<Tuple<Integer, McskyProudData>>>> windowedStage) throws UnsupportedSpaceException {
        createDistributableData();
        final DistributedMap<String, PMCSkyState> stateMap = new DistributedMap<>(STATES_KEY);

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

        final int slide = proudContext.internalConfiguration().getCommonS();

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

        List<Integer> W_distinct_list = outlierQueries.stream()
                .map(OutlierQuery::getWindow)
                .distinct()
                .sorted()
                .collect(Collectors.toList());

        List<Integer> S_distinct_list = outlierQueries.stream()
                .map(OutlierQuery::getSlide)
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

        final int w_min = W_distinct_list.stream()
                .min(Comparator.naturalOrder())
                .orElse(0);

        final int k_size = k_distinct_list.size();
        final int R_size = R_distinct_list.size();
        final int W_size = W_distinct_list.size();
        final int S_size = S_distinct_list.size();

        final List<Integer> S_report_times = S_distinct_list.stream()
                .map((it)->it/slide)
                .sorted()
                .collect(Collectors.toList());
        final int S_max_report = S_report_times.stream()
                .mapToInt(Integer::intValue)
                .max().orElse(0);

        StreamStage<AppendableTraverser<Tuple<Long,OutlierQuery>>> detectOutliersStage = windowedStage.rollingAggregate(
                components.outlierDetection((outliers, window)->{
                    // Detect outliers and add them to outliers accumulator
                    int partition = window.getKey();

                    long windowStart = window.start();
                    long windowEnd = window.end();
                    long latestSlide = windowEnd - slide;

                    final String STATE_KEY = "STATE";

                    List<McskyProudData> elements = window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((it)->it.arrival >= windowEnd - slide)
                            .collect(Collectors.toList());

                    PMCSkyState current = stateMap.get(STATE_KEY);

                    if (current == null) {
                        current = new PMCSkyState();
                        current.slideCount = windowEnd / slide;
                        stateMap.put(STATE_KEY, current);
                    }

                    final KeyedWindow<McskyProudData> windowRef = new KeyedWindow<>(partition, windowStart, windowEnd);
                    final RKWS_PMCSky pmcsky = new RKWS_PMCSky(current, windowRef, R_distinct_list, slide, R_min, R_max, k_max, w_min);

                    int[][][] allQueries = ArrayUtils.multidimensionalWith(0, R_size, k_size, W_size);

                    List<Integer> output_slide = Lists.make();
                    for (int reportTime: S_report_times) {
                        if (current.slideCount % reportTime == 0)
                            output_slide.add(reportTime);
                    }

                    // Insert new elements to state
                    List<McskyProudData> elementsToAdd = elements.stream()
                            //Sort is needed when each point has a different timestamp
                            //In our case every point in the same slide has the same timestamp
                            .sorted(Comparator.comparingLong((it)->it.arrival))
                            .collect(Collectors.toList());

                    for (McskyProudData element:elementsToAdd) {
                        current.index.put(element.id, element);
                    }

                    // Update elements
                    window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((it)->!it.safe_inlier && it.flag == 0 && it.mc == -1)
                            .forEach((p)-> {
                                pmcsky.checkPoint(p);

                                if (p.mc == -1) {
                                    boolean isSafeInlier = p.lsky.getOrDefault(1, Lists.make()).stream()
                                            .filter((it)->it.second >= p.arrival)
                                            .count() >= k_max;

                                    if (isSafeInlier) {
                                        p.safe_inlier = true;
                                    } else {

                                        if (!output_slide.isEmpty()) {
                                            int w = 0;

                                            do {
                                                final int currentW = W_distinct_list.get(w);

                                                if (p.arrival >= windowEnd - currentW) {
                                                    int i = 0;
                                                    int y = 0;

                                                    long count = p.lsky.getOrDefault(i+1, Lists.make()).stream()
                                                            .filter((it)->it.second >= windowEnd - currentW)
                                                            .count();

                                                    do {
                                                        if(count >= k_distinct_list.get(y)){ // Inlier for all i
                                                            y += 1;
                                                        } else {  // Outlier for all y
                                                            for(int z=y; z < k_size; z++){
                                                                allQueries[i][z][w] += 1;
                                                            }

                                                            i += 1;
                                                            count += p.lsky.getOrDefault(i+1, Lists.make()).stream()
                                                                    .filter((it)->it.second >= windowEnd - currentW)
                                                                    .count();
                                                        }

                                                    } while (i < R_size && y < k_size);
                                                }

                                                w++;
                                            } while (w<W_size);
                                        }
                                    }
                                }
                            });

                    // Report outliers
                    if (!output_slide.isEmpty()) {
                        List<Integer> reportingSlides = output_slide.stream()
                                .map((it)->it*slide)
                                .collect(Collectors.toList());

                        for (int i=0; i < R_size; i++){
                            for (int y=0; y < k_size; y++){
                                for (int z=0; z < W_size; z++) {
                                    for (int currentSlide:reportingSlides) {
                                        OutlierQuery outlierQuery = new OutlierQuery(
                                                R_distinct_list.get(i),
                                                k_distinct_list.get(y),
                                                W_distinct_list.get(z),
                                                currentSlide
                                        ).withOutlierCount(allQueries[i][y][z]);
                                        outliers.append(new Tuple<>(windowEnd, outlierQuery));
                                    }
                                }
                            }
                        }

                    }

                    current.slideCount += 1;

                    // Remove old points
                    window.getValue().stream()
                            .map(Tuple::getSecond)
                            .forEach((el)-> {
                                //Remove small window points from clusters
                                if(el.mc != -1 && el.arrival >= windowEnd - w_min)
                                    pmcsky.deleteSmallWindowPoint(el);
                                //Remove old points
                                if (el.arrival < windowStart + slide)
                                    pmcsky.deletePoint(el);
                            });

                    // If micro-cluster is needed as part of the distributed state remove the following line
                    current.mcCounter = new AtomicInteger(1);
                    stateMap.put(STATE_KEY, current);
                })
        );

        // Return flattened stream
        StreamStage<Tuple<Long, OutlierQuery>> flattenedResult = detectOutliersStage.flatMap((it)->it);
        return flattenedResult;
    }

    private abstract static class PMCSky implements Serializable
    {
        public PMCSkyState state;
        public KeyedWindow<McskyProudData> window;

        public LinkedList<Double> R_distinct_list;

        public int slide;

        public double R_min;
        public double R_max;
        public int K_max;

        public PMCSky(PMCSkyState state, KeyedWindow<McskyProudData> window, List<Double> r_distinct_list, int slide, double r_min, double r_max, int k_max) {
            this.state = state;
            this.window = window;
            R_distinct_list = new LinkedList<>(r_distinct_list);
            this.slide = slide;
            R_min = r_min;
            R_max = r_max;
            K_max = k_max;
        }

        public void checkPoint(McskyProudData el) {
            if (el.lsky.isEmpty() && el.mc == -1) { //It's a new point
                insertPoint(el);
            } else if(el.mc == -1) { //It's an old point
                updatePoint(el);
            }
        }

        public abstract void insertPoint(McskyProudData el);

        public abstract void updatePoint(McskyProudData el);

        public Map<Integer,Double> findCloseMicroClusters(McskyProudData el) {
            Map<Integer,Double> res = new HashMap<>();

            state.mc.entrySet().stream()
                    .map((entry) -> new Tuple<>(entry.getKey(), Distances.distanceOf(el, new EuclideanCoordinateList<>(entry.getValue().center))))
                    .filter((it)-> it.second <= (3 * R_max) / 2)
                    .forEach((it)->res.put(it.first, it.second));

            return res;
        }

        public void insertToMicroCluster(McskyProudData el, int mc) {
            el.clear(mc);

            state.mc.get(mc).points.add(el.id);
            state.pd.remove(el.id);
        }

        public void createMicroCluster(McskyProudData el, List<McskyProudData> NC) {
            int mcCounter = state.mcCounter.get();

            NC.add(el);

            for (McskyProudData it:NC) {
                it.clear(mcCounter);
                state.pd.remove(it.id);
            }

            List<Integer> NCIds = NC.stream()
                    .map(McskyProudData::getId)
                    .collect(Collectors.toList());

            PMCSkyCluster newMC = new PMCSkyCluster(el.value, NCIds);
            state.mc.put(mcCounter, newMC);

            mcCounter += 1;
            state.mcCounter.set(mcCounter);
        }

        public abstract void deletePoint(McskyProudData el);

        public boolean neighbourSkyband(McskyProudData el, McskyProudData neighbour, double distance) {
            double normalizedDistance = normalizeDistance(distance);

            int count = 0;

            for (int i=1;i<normalizedDistance;i++) {
                count += el.lsky.getOrDefault(i, new ArrayList<>()).size();
            }

            if (count <= K_max-1) {
                List<Tuple<Integer, Long>> none = el.lsky
                        .getOrDefault((int) normalizedDistance, new ArrayList<>());
                none.add(new Tuple<>(neighbour.id, neighbour.arrival));

                el.lsky.put((int) normalizedDistance,none);
                return true;
            }

            return false;
        }

        public double normalizeDistance(double distance) {
            double normalizedDistance = 0.0;
            int i = 0;

            do {
                if (distance <= R_distinct_list.get(i))
                    normalizedDistance = i + 1;

                i += 1;
            } while (i < R_distinct_list.size() && normalizedDistance == 0);

            return normalizedDistance;
        }
    }

    private final static class RK_PMCSky extends PMCSky
    {

        public RK_PMCSky(PMCSkyState state, KeyedWindow<McskyProudData> window, List<Double> r_distinct_list, int slide, double r_min, double r_max, int k_max) {
            super(state, window, r_distinct_list, slide, r_min, r_max, k_max);
        }

        @Override
        public void insertPoint(McskyProudData el) {
            Map<Integer, Double> closeMicroClusters = findCloseMicroClusters(el);
            Tuple<Integer, Double> closestMC = closeMicroClusters.entrySet().stream()
                    .map(Tuple::fromEntry)
                    .min(Comparator.comparingDouble(Tuple::getSecond))
                    .orElse(new Tuple<>(0, Double.MAX_VALUE));

            if (closestMC.second < R_min / 2.0) { //Insert element to MC
                insertToMicroCluster(el, closestMC.first);
            } else { // Check against PD
                // List to hold points for new cluster formation
                List<McskyProudData> NC = new ArrayList<>();
                List<McskyProudData> items = Lists.reversing(state.index.values());

                // Find the points so far from latest to earliest
                for(McskyProudData p:items) {
                    if (p.id != el.id) {
                        if (closeMicroClusters.containsKey(p.mc) || p.mc == -1) {
                            double distance = Distances.distanceOf(p, el);
                            if (distance <= R_max) {
                                if (p.mc == -1 && distance <= R_min / 2)
                                    NC.add(p);

                                if (!neighbourSkyband(el,p,distance) && distance <= R_min)
                                    break;
                            }
                        }
                    }
                }

                if (NC.size() >= K_max) { //Create new MC
                    createMicroCluster(el, NC);
                }
                else { //Insert in PD
                    state.pd.add(el.id);
                }

            }
        }

        @Override
        public void updatePoint(McskyProudData el) {
            //Remove old points from lSky
            for(Integer key:el.lsky.keySet()) {
                List<Tuple<Integer, Long>> value = el.lsky.get(key).stream()
                        .filter((it) -> it.second >= window.start)
                        .collect(Collectors.toList());

                el.lsky.put(key, value);
            }

            //Create input
            List<Integer> oldSky = el.lsky.values().stream().flatMap(Collection::stream)
                    .sorted(Comparator.comparingLong(Tuple::getSecond))
                    .map(Tuple::getFirst)
                    .collect(Collectors.toList());
            el.lsky.clear();

            // Variable to stop skyband loop
            boolean resultFlag = true;
            // List to hold points for new cluster formation
            List<McskyProudData> NC = Lists.make();

            // Check new points
            for (McskyProudData data : Lists.reversing(state.index.values())) {
                boolean innerResultFlag = true;

                if (data.arrival >= window.end - slide) {
                    double distance = Distances.distanceOf(el, data);

                    if (distance <= R_max) {
                        if (!neighbourSkyband(el, data, distance) && distance <= R_min) {
                            resultFlag = false;
                        }

                        if (state.pd.contains(data.id) && distance <= R_min / 2.0)
                            NC.add(data);
                    }

                } else {
                    innerResultFlag = false;
                }

                if (resultFlag && innerResultFlag)
                    continue;

                break;
            }

            // //Check the old skyband elements
            for (Integer id:oldSky) {

                if (!resultFlag)
                    break;

                McskyProudData p = state.index.get(id);
                double distance = Distances.distanceOf(el, p);

                if (distance <= R_max) {
                    if (!neighbourSkyband(el, p, distance) && distance < R_min) {
                        resultFlag = false;
                    }

                    if (state.pd.contains(p.id) && distance <= R_min / 2)
                        NC.add(p);
                }
            }

            if (NC.size() >= K_max) {
                createMicroCluster(el, NC); //Create new MC
            } else {
                state.pd.add(el.id); //Insert in PD
            }
        }

        @Override
        public void deletePoint(McskyProudData el) {
            if (el.mc == -1) { //Delete it from PD
                state.pd.remove(el.id);
            } else {
                state.mc.get(el.mc).points.remove(el.id);

                if (state.mc.get(el.mc).points.size() <= K_max) {
                    state.mc.get(el.mc).points.forEach(p -> {
                            state.index.get(p).clear(-1);
                    });

                    state.mc.remove(el.mc);
                }
            }

            state.index.remove(el.id);
        }
    }

    private final static class RKWS_PMCSky extends PMCSky
    {
        public int W_min;

        public RKWS_PMCSky(PMCSkyState state, KeyedWindow<McskyProudData> window, List<Double> r_distinct_list, int slide, double r_min, double r_max, int k_max, int w_min) {
            super(state, window, r_distinct_list, slide, r_min, r_max, k_max);
            W_min = w_min;
        }

        @Override
        public void insertPoint(McskyProudData el) {
            boolean smallWindow = el.arrival >= window.end - W_min;

            Map<Integer, Double> closeMicroClusters = findCloseMicroClusters(el);
            Tuple<Integer, Double> closestMC = closeMicroClusters.entrySet().stream()
                    .map(Tuple::fromEntry)
                    .min(Comparator.comparingDouble(Tuple::getSecond))
                    .orElse(new Tuple<>(0, Double.MAX_VALUE));

            if (closestMC.second < R_min / 2.0 && smallWindow) { //Insert element to MC
                // Create MCs only on the smaller window parameter
                insertToMicroCluster(el, closestMC.first);
            } else { // Check against PD
                // List to hold points for new cluster formation
                List<McskyProudData> NC = new ArrayList<>();
                List<McskyProudData> items = Lists.reversing(state.index.values());

                // Find the points so far from latest to earliest
                for(McskyProudData p:items) {

                    if (p.id != el.id) {
                        if (closeMicroClusters.containsKey(p.mc) || p.mc == -1) {
                            double distance = Distances.distanceOf(p, el);
                            if (distance <= R_max) {
                                if (p.arrival >= window.end - W_min && p.mc == -1 && distance <= R_min / 2)
                                    NC.add(p);

                                if (!neighbourSkyband(el,p,distance) && distance <= R_min)
                                    break;
                            }
                        }
                    }

                }

                if (smallWindow && NC.size() >= K_max) { //Create new MC
                    createMicroCluster(el, NC);
                }
                else { //Insert in PD
                    state.pd.add(el.id);
                }

            }
        }

        @Override
        public void updatePoint(McskyProudData el) {
            boolean smallWindow = el.arrival >= window.end - W_min;

            //Remove old points from lSky
            for(Integer key:el.lsky.keySet()) {
                List<Tuple<Integer, Long>> value = el.lsky.get(key).stream()
                        .filter((it) -> it.second >= window.start)
                        .collect(Collectors.toList());

                el.lsky.put(key, value);
            }

            //Create input
            List<Integer> oldSky = el.lsky.values().stream().flatMap(Collection::stream)
                    .sorted(Comparator.comparingLong(Tuple::getSecond))
                    .map(Tuple::getFirst)
                    .collect(Collectors.toList());
            el.lsky.clear();

            // Variable to stop skyband loop
            boolean resultFlag = true;
            // List to hold points for new cluster formation
            List<McskyProudData> NC = Lists.make();

            // Check new points
            for (McskyProudData data : Lists.reversing(state.index.values())) {
                boolean innerResultFlag = true;

                if (data.arrival >= window.end - slide) {
                    double distance = Distances.distanceOf(el, data);

                    if (distance <= R_max) {
                        if (!neighbourSkyband(el, data, distance) && distance <= R_min) {
                            resultFlag = false;
                        }

                        if (state.pd.contains(data.id) && distance <= R_min / 2.0)
                            NC.add(data);
                    }

                } else {
                    innerResultFlag = false;
                }

                if (resultFlag && innerResultFlag)
                    continue;

                break;
            }

            // //Check the old skyband elements
            for (Integer id:oldSky) {

                if (!resultFlag)
                    break;

                McskyProudData p = state.index.get(id);
                double distance = Distances.distanceOf(el, p);

                if (distance <= R_max) {
                    if (!neighbourSkyband(el, p, distance) && distance < R_min) {
                        resultFlag = false;
                    }

                    if (p.arrival >= window.end - W_min && state.pd.contains(p.id) && distance <= R_min / 2)
                        NC.add(p);
                }
            }

            if (smallWindow && NC.size() >= K_max) {
                createMicroCluster(el, NC); //Create new MC
            } else {
                state.pd.add(el.id); //Insert in PD
            }

        }

        @Override
        public void deletePoint(McskyProudData el) {
            if (el.mc == -1) { // Delete it from PD
                state.pd.remove(el.id);
            }

            state.index.remove(el.id);
        }

        public void deleteSmallWindowPoint(McskyProudData el) {
            if (el.mc != -1) { // Delete it from MC
                state.mc.get(el.mc).points.remove(el.id);

                if (state.mc.get(el.mc).points.size() <= K_max) {
                    state.mc.get(el.mc).points
                            .forEach(p -> state.index.get(p).clear(-1));

                    state.mc.remove(el.mc);
                }

                el.clear(-1);
            }
        }
    }
}
