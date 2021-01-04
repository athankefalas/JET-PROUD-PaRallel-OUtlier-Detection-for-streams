package edu.auth.jetproud.proud.algorithms.executors;

import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.application.config.ProudConfiguration;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.application.parameters.data.ProudSpaceOption;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.LSKYProudData;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.algorithms.AnyProudAlgorithmExecutor;
import edu.auth.jetproud.proud.algorithms.Distances;
import edu.auth.jetproud.proud.algorithms.exceptions.UnsupportedSpaceException;
import edu.auth.jetproud.proud.algorithms.functions.ProudComponentBuilder;
import edu.auth.jetproud.proud.distributables.DistributedCounter;
import edu.auth.jetproud.proud.distributables.DistributedMap;
import edu.auth.jetproud.proud.distributables.KeyedStateHolder;
import edu.auth.jetproud.proud.metrics.ProudStatistics;
import edu.auth.jetproud.utils.ArrayUtils;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class PSODProudAlgorithmExecutor extends AnyProudAlgorithmExecutor<LSKYProudData>
{

    static class PSODState implements Serializable
    {
        public ConcurrentHashMap<Integer, LSKYProudData> index;
        public AtomicLong slideCount;

        public PSODState(HashMap<Integer, LSKYProudData> index) {
            this(index, 1);
        }

        public PSODState(HashMap<Integer, LSKYProudData> index, long slideCount) {
            this.slideCount = new AtomicLong(slideCount);
            this.index = new ConcurrentHashMap<>(index);
        }
    }

    public PSODProudAlgorithmExecutor(ProudContext proudContext) {
        super(proudContext, ProudAlgorithmOption.PSod);
    }

    @Override
    protected <D extends AnyProudData> LSKYProudData transform(D point) {
        return new LSKYProudData(point);
    }

    @Override
    public List<ProudSpaceOption> supportedSpaceOptions() {
        return Lists.of(
                ProudSpaceOption.MultiQueryMultiParams,
                ProudSpaceOption.MultiQueryMultiParamsMultiWindowParams
        );
    }

    @Override
    protected StreamStage<Tuple<Long, OutlierQuery>> processMultiQueryParamsSpace(StreamStage<KeyedWindowResult<Integer, List<Tuple<Integer, LSKYProudData>>>> windowedStage) throws UnsupportedSpaceException {
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

        return windowedStage.flatMapStateful(()-> KeyedStateHolder.<String, PSODState>create(),
                (stateHolder, window) -> {
                    // Statistics
                    DistributedCounter slideCounter = ProudStatistics.slideCounter();
                    DistributedCounter cpuTimeCounter = ProudStatistics.cpuTimeCounter();

                    slideCounter.incrementAndGet();
                    long startTime = System.currentTimeMillis();

                    // Detect outliers and add them to outliers accumulator
                    List<Tuple<Long,OutlierQuery>> outliers = Lists.make();
                    int partition = window.getKey();

                    long windowStart = window.start();
                    long windowEnd = window.end();

                    final String STATE_KEY = "STATE_"+partition;

                    List<LSKYProudData> elements = window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((it)->it.arrival >= windowEnd - slide)
                            .collect(Collectors.toList());

                    PSODState current = stateHolder.get(STATE_KEY);

                    if (current == null) {
                        current = new PSODState(new HashMap<>());
                        stateHolder.put(STATE_KEY, current);
                    }

                    int[][] allQueries = ArrayUtils.multidimensionalWith(0, R_size, k_size);
                    final PSOD psod = new PSOD(current, R_distinct_list, R_max, k_max);

                    // Remove old points from each lSky
                    current.index.values().forEach((p) -> {
                        p.lsky.replaceAll((key, value)->{
                            return value.stream()
                                    .filter((it)->it.second >= windowStart)
                                    .collect(Collectors.toList());
                        });
                    });

                    // Insert new elements
                    for (LSKYProudData element : elements) {
                        psod.insertPoint(element);
                    }

                    // Update elements
                    window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((it)->!it.safe_inlier && it.flag == 0)
                            .forEach((p)-> {
                                boolean isSafeInlier = p.lsky.getOrDefault(0, Lists.make()).stream()
                                        .filter((it)->it.second >= p.arrival)
                                        .count() >= k_max;

                                if (isSafeInlier) {
                                    p.safe_inlier = true;
                                } else {
                                    int i = 0;
                                    int y = 0;

                                    long count = p.lsky.getOrDefault(i, Lists.make()).size();

                                    do {
                                        if(count >= k_distinct_list.get(y)){ // Inlier for all i
                                            y += 1;
                                        } else {  // Outlier for all y
                                            for(int z=y; z < k_size; z++){
                                                allQueries[i][z] += 1;
                                            }

                                            i += 1;
                                            count += p.lsky.getOrDefault(i, Lists.make()).size();
                                        }

                                    }while (i < R_size && y < k_size);
                                }
                            });

                    // Report outliers
                    for (int i=0; i < R_size; i++){
                        for (int y=0; y < k_size; y++){
                            OutlierQuery outlierQuery = new OutlierQuery(
                                    R_distinct_list.get(i),
                                    k_distinct_list.get(y),
                                    outlierQueries.get(0).window,
                                    outlierQueries.get(0).slide
                            ).withOutlierCount(allQueries[i][y]);
                            outliers.add(new Tuple<>(windowEnd, outlierQuery));
                        }
                    }

                    // Remove Old Elements
                    window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((it)-> it.arrival < windowStart + slide)
                            .forEach(psod::deletePoint);


                    // Statistics
                    long endTime = System.currentTimeMillis();
                    long duration = endTime - startTime;
                    cpuTimeCounter.addAndGet(duration);

                    // Return results
                    return Traversers.traverseIterable(outliers);
                });
    }

    @Override
    protected StreamStage<Tuple<Long, OutlierQuery>> processMultiQueryParamsMultiWindowParamsSpace(StreamStage<KeyedWindowResult<Integer, List<Tuple<Integer, LSKYProudData>>>> windowedStage) throws UnsupportedSpaceException {
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

        final int k_size = k_distinct_list.size();
        final int R_size = R_distinct_list.size();
        final int W_size = W_distinct_list.size();
        final int S_size = S_distinct_list.size();
        final List<Integer> S_distinct_downgraded = S_distinct_list.stream()
                .map((it)-> it/slide)
                .distinct()
                .collect(Collectors.toList());

        int product = S_distinct_downgraded.stream().reduce(1,(a,b)->a*b);
        final List<Long> S_var = Lists.range(1, product + 1).stream()
                .filter((i)-> {
                    return S_distinct_downgraded.stream()
                            .anyMatch((it) -> i % it == 0);
                })
                .distinct()
                .sorted()
                .map(Integer::longValue)
                .collect(Collectors.toList());

        long S_var_max = S_var.stream()
                .mapToLong(Long::longValue)
                .max().orElse(0);

        return windowedStage.flatMapStateful(()->KeyedStateHolder.<String, PSODState>create(),
                (stateHolder, window) -> {
                    // Statistics
                    DistributedCounter slideCounter = ProudStatistics.slideCounter();
                    DistributedCounter cpuTimeCounter = ProudStatistics.cpuTimeCounter();

                    slideCounter.incrementAndGet();
                    long startTime = System.currentTimeMillis();

                    // Detect outliers and add them to outliers accumulator
                    List<Tuple<Long,OutlierQuery>> outliers = Lists.make();
                    int partition = window.getKey();

                    long windowStart = window.start();
                    long windowEnd = window.end();

                    final String STATE_KEY = "STATE_"+partition;

                    List<LSKYProudData> elements = window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((it)->it.arrival >= windowEnd - slide)
                            .collect(Collectors.toList());

                    PSODState current = stateHolder.get(STATE_KEY);

                    if (current == null) {
                        long currentSlide = windowEnd / slide;
                        current = new PSODState(new HashMap<>(), currentSlide);
                        stateHolder.put(STATE_KEY, current);
                    }

                    final long slideCount = current.slideCount.get();

                    int[][][] allQueries = ArrayUtils.multidimensionalWith(0, R_size, k_size, W_size);
                    final PSOD psod = new PSOD(current, R_distinct_list, R_max, k_max);

                    // Remove old points from each lSky
                    current.index.values().forEach((p) -> {
                        p.lsky.replaceAll((key, value)->{
                            return value.stream()
                                    .filter((it)->it.second >= windowStart)
                                    .collect(Collectors.toList());
                        });
                    });

                    // Insert new elements
                    for (LSKYProudData element : elements) {
                        psod.insertPoint(element);
                    }

                    // Update elements

                    window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((it)->!it.safe_inlier && it.flag == 0)
                            .forEach((p)-> {

                                boolean isSafeInlier = p.lsky.getOrDefault(0, Lists.make()).stream()
                                        .filter((it)->it.second >= p.arrival)
                                        .count() >= k_max;

                                if (isSafeInlier) {
                                    p.safe_inlier = true;
                                } else {

                                    if (S_var.contains(slideCount)) {
                                        int w = 0;

                                        do {
                                            final int currentW = W_distinct_list.get(w);

                                            if (p.arrival >= windowEnd - currentW) {
                                                int i = 0;
                                                int y = 0;

                                                long count = p.lsky.getOrDefault(i, Lists.make()).stream()
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
                                                        count += p.lsky.getOrDefault(i, Lists.make()).stream()
                                                                .filter((it)->it.second >= windowEnd - currentW)
                                                                .count();
                                                    }

                                                }while (i < R_size && y < k_size);
                                            }

                                            w++;
                                        }while (w < W_size);
                                    }
                                }
                            });

                    // Report outliers
                    if (S_var.contains(slideCount)) {
                        List<Integer> reportingSlides = S_distinct_downgraded.stream()
                                .filter((it)-> slideCount % it == 0)
                                .map((it)->it * slide)
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
                                        outliers.add(new Tuple<>(windowEnd, outlierQuery));
                                    }
                                }
                            }
                        }

                    }

                    current.slideCount.set(slideCount + 1);

                    if(current.slideCount.get() > S_var_max)
                        current.slideCount.set(1);

                    // Remove Old Elements
                    window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((it)-> it.arrival < windowStart + slide)
                            .forEach(psod::deletePoint);

                    // Statistics
                    long endTime = System.currentTimeMillis();
                    long duration = endTime - startTime;
                    cpuTimeCounter.addAndGet(duration);

                    // Return results
                    return Traversers.traverseIterable(outliers);
                });
    }

    private static final class PSOD implements Serializable
    {
        public PSODState state;

        public LinkedList<Double> R_distinct_list;

        public double R_max;
        public int K_max;

        public PSOD(PSODState state, List<Double> r_distinct_list, double r_max, int k_max) {
            this.state = state;
            R_distinct_list = new LinkedList<>(r_distinct_list);
            R_max = r_max;
            K_max = k_max;
        }

        public void insertPoint(LSKYProudData el) {
            // Get the points so far from latest to earliest
            List<LSKYProudData> points = state.index.values().stream()
                    .sorted(Comparator.comparingLong(LSKYProudData::getArrival).reversed())
                    .collect(Collectors.toList());

            for (LSKYProudData p:points) {
                if (p.id == el.id)
                    continue;

                double distance = Distances.distanceOf(el, p);

                if (distance <= R_max) {
                    addNeighbour(el, p, distance);
                }
            }

            state.index.put(el.id, el);
        }

        public void deletePoint(LSKYProudData el) {
            state.index.remove(el.id);
        }

        public void addNeighbour(LSKYProudData el, LSKYProudData neigh, double distance) {
            int norm_dist = normalizeDistance(distance);

            if (el.flag == 0 && el.lsky.getOrDefault(norm_dist, Lists.make()).size() < K_max) {
                List<Tuple<Integer, Long>> value = el.lsky.getOrDefault(norm_dist, Lists.make());
                value.add(new Tuple<>(neigh.id, neigh.arrival));

                el.lsky.put(norm_dist, value);
            }else if(el.flag == 0 && el.lsky.getOrDefault(norm_dist, Lists.make()).size() == K_max){
                Tuple<Integer, Long> min = el.lsky.get(norm_dist).stream()
                        .min(Comparator.comparingLong(Tuple::getSecond))
                        .orElse(new Tuple<>(-1, 0L));

                int minId = min.first;
                long minArrival = min.second;

                if(neigh.arrival > minArrival) {
                    List<Tuple<Integer, Long>> value = el.lsky.getOrDefault(norm_dist, Lists.make()).stream()
                            .filter((it)->it.first != minId)
                            .collect(Collectors.toList());
                    value.add(new Tuple<>(neigh.id, neigh.arrival));

                    el.lsky.put(norm_dist, value);
                }
            }

            if (!neigh.safe_inlier) {
                if (neigh.flag == 0 && neigh.lsky.getOrDefault(norm_dist, Lists.make()).size() < K_max) {
                    List<Tuple<Integer, Long>> value = neigh.lsky.getOrDefault(norm_dist, Lists.make());
                    value.add(new Tuple<>(el.id, el.arrival));

                    neigh.lsky.put(norm_dist, value);
                }else if(neigh.flag == 0 && neigh.lsky.getOrDefault(norm_dist, Lists.make()).size() == K_max){
                    Tuple<Integer, Long> min = neigh.lsky.getOrDefault(norm_dist, Lists.make()).stream()
                            .min(Comparator.comparingLong(Tuple::getSecond))
                            .orElse(new Tuple<>(-1, 0L));

                    int minId = min.first;
                    long minArrival = min.second;

                    if(el.arrival > minArrival) {
                        List<Tuple<Integer, Long>> value = neigh.lsky.getOrDefault(norm_dist, Lists.make()).stream()
                                .filter((it)->it.first != minId)
                                .collect(Collectors.toList());
                        value.add(new Tuple<>(el.id, el.arrival));

                        neigh.lsky.put(norm_dist, value);
                    }
                }
            }
        }

        public int normalizeDistance(double distance) {
            int normalizedDistance = -1;
            int i = 0;

            do {
                if (distance <= R_distinct_list.get(i))
                    normalizedDistance = i;

                i += 1;
            } while (i < R_distinct_list.size() && normalizedDistance == -1);

            return normalizedDistance;
        }
    }

}
