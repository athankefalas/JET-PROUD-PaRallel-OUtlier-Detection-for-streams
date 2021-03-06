package edu.auth.jetproud.proud.algorithms.executors;

import com.hazelcast.jet.Traversers;
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
import edu.auth.jetproud.proud.algorithms.ProudKeyedWindow;
import edu.auth.jetproud.proud.algorithms.exceptions.UnsupportedSpaceException;
import edu.auth.jetproud.proud.algorithms.functions.ProudComponentBuilder;
import edu.auth.jetproud.proud.distributables.KeyedStateHolder;
import edu.auth.jetproud.utils.ArrayUtils;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class SOPProudAlgorithmExecutor extends AnyProudAlgorithmExecutor<LSKYProudData>
{

    static class SOPState implements Serializable
    {
        public ConcurrentHashMap<Integer, LSKYProudData> index;
        public AtomicLong slideCount;

        public SOPState(HashMap<Integer, LSKYProudData> index) {
            this(index, 1);
        }

        public SOPState(HashMap<Integer, LSKYProudData> index, long slideCount) {
            this.slideCount = new AtomicLong(slideCount);
            this.index = new ConcurrentHashMap<>(index);
        }
    }

    public SOPProudAlgorithmExecutor(ProudContext proudContext) {
        super(proudContext, ProudAlgorithmOption.Sop);
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

        final List<Double> R_distinct_list = outlierQueries.stream()
                .map(OutlierQuery::getRange)
                .distinct()
                .sorted()
                .collect(Collectors.toList());

        final List<Integer> k_distinct_list = outlierQueries.stream()
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

        return windowedStage.flatMapStateful(()-> KeyedStateHolder.<String, SOPState>create(),
                (stateHolder, window) -> {
                    // Metrics & Statistics
                    SlideMetricsRecorder metricsRecorder = startRecordingMetrics();

                    // Detect outliers and add them to outliers accumulator
                    List<Tuple<Long,OutlierQuery>> outliers = Lists.make();
                    int partition = window.getKey();

                    long windowStart = window.start();
                    long windowEnd = window.end();
                    long latestSlide = windowEnd - slide;

                    final String STATE_KEY = "STATE_"+partition;

                    List<LSKYProudData> elements = window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((it)->it.arrival >= windowEnd - slide)
                            .collect(Collectors.toList());

                    SOPState current = stateHolder.get(STATE_KEY);

                    if (current == null) {
                        current = new SOPState(new HashMap<>());
                        stateHolder.put(STATE_KEY, current);
                    }

                    final ProudKeyedWindow<LSKYProudData> windowRef = new ProudKeyedWindow<>(partition, windowStart, windowEnd, elements);
                    final SOP sop = new SOP(current, windowRef, R_distinct_list, slide, R_min, R_max, k_min, k_max);

                    int[][] allQueries = ArrayUtils.multidimensionalWith(0, R_size, k_size);

                    // Insert new elements
                    List<LSKYProudData> sortedElements = elements.stream()
                            .sorted(Comparator.comparingLong(LSKYProudData::getArrival))
                            .collect(Collectors.toList());

                    for (LSKYProudData el:sortedElements) {
                        current.index.put(el.id, el);
                    }

                    // Update elements
                    window.getValue().stream()
                            .map(Tuple::getSecond)
                            .forEach((p)->{
                                if (!p.safe_inlier && p.flag == 0) {
                                    sop.checkPoint(p);

                                    boolean isSafeInlier = p.lsky.getOrDefault(1, Lists.make()).stream()
                                            .filter((it)->it.second >= p.arrival)
                                            .count() >= k_max;

                                    if (isSafeInlier) {
                                        p.safe_inlier = true;
                                    } else {

                                        int i = 0;
                                        int y = 0;

                                        long count = p.lsky.getOrDefault(i+1, Lists.make()).stream()
                                                .filter((it)->it.second >= windowStart)
                                                .count();

                                        do {
                                            if(count >= k_distinct_list.get(y)) { // Inlier for all i
                                                y += 1;
                                            } else {  // Outlier for all y
                                                for(int z=y; z < k_size; z++) {
                                                    allQueries[i][z] += 1;
                                                }

                                                i += 1;
                                                count += p.lsky.getOrDefault(i+1, Lists.make()).stream()
                                                        .filter((it)->it.second >= windowStart)
                                                        .count();
                                            }

                                        }while (i < R_size && y < k_size);
                                    }
                                }
                            });

                    // Report outliers
                    for (int i=0; i < R_size; i++) {
                        for (int y=0; y < k_size; y++) {
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
                            .forEach(sop::deletePoint);

                    stateHolder.put(STATE_KEY, current);

                    // Metrics & Statistics
                    stopRecordingMetrics(metricsRecorder);

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

        final List<Integer> S_report_times = S_distinct_list.stream()
                .map((it)->it/slide)
                .sorted()
                .collect(Collectors.toList());

        final int S_max_report = S_report_times.stream()
                .mapToInt(Integer::intValue)
                .max()
                .orElse(0);

        return windowedStage.flatMapStateful(()->KeyedStateHolder.<String, SOPState>create(),
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

                    List<LSKYProudData> elements = window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((it)->it.arrival >= windowEnd - slide)
                            .collect(Collectors.toList());

                    SOPState current = stateHolder.get(STATE_KEY);

                    if (current == null) {
                        long currentSlide = windowEnd / slide;
                        current = new SOPState(new HashMap<>(), currentSlide);
                        stateHolder.put(STATE_KEY, current);
                    }

                    final ProudKeyedWindow<LSKYProudData> windowRef = new ProudKeyedWindow<>(partition, windowStart, windowEnd, elements);
                    final SOP sop = new SOP(current, windowRef, R_distinct_list, slide, R_min, R_max, k_min, k_max);

                    List<Integer> output_slide = Lists.make();
                    for (int reportTime: S_report_times) {
                        if (current.slideCount.get() % reportTime == 0)
                            output_slide.add(reportTime);
                    }

                    int[][][] allQueries = ArrayUtils.multidimensionalWith(0, R_size, k_size, W_size);

                    // Insert new elements
                    List<LSKYProudData> sortedElements = elements.stream()
                            .sorted(Comparator.comparingLong(LSKYProudData::getArrival))
                            .collect(Collectors.toList());

                    for (LSKYProudData el:sortedElements) {
                        current.index.put(el.id, el);
                    }

                    // Update elements
                    window.getValue().stream()
                            .map(Tuple::getSecond)
                            .forEach((p)->{
                                if (!p.safe_inlier && p.flag == 0) {
                                    sop.checkPoint(p);

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

                                                    }while (i < R_size && y < k_size);
                                                }

                                                w++;
                                            }while (w < W_size);
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
                                        outliers.add(new Tuple<>(windowEnd, outlierQuery));
                                    }
                                }
                            }
                        }

                    }

                    current.slideCount.incrementAndGet();

                    // Remove Old Elements
                    window.getValue().stream()
                            .map(Tuple::getSecond)
                            .filter((it)-> it.arrival < windowStart + slide)
                            .forEach(sop::deletePoint);


                    stateHolder.put(STATE_KEY, current);

                    // Metrics & Statistics
                    stopRecordingMetrics(metricsRecorder);

                    // Return results
                    return Traversers.traverseIterable(outliers);
                });
    }

    private static final class SOP implements Serializable
    {
        public SOPState state;
        public ProudKeyedWindow<LSKYProudData> window;

        public LinkedList<Double> R_distinct_list;

        public int slide;

        public double R_min;
        public double R_max;
        public int K_min;
        public int K_max;

        public SOP(SOPState state, ProudKeyedWindow<LSKYProudData> window, List<Double> r_distinct_list, int slide, double r_min, double r_max, int k_min, int k_max) {
            this.state = state;
            this.window = window;

            R_distinct_list = new LinkedList<>(r_distinct_list);

            this.slide = slide;
            R_min = r_min;
            R_max = r_max;
            K_min = k_min;
            K_max = k_max;
        }

        public void checkPoint(LSKYProudData el) {
            if (el.lsky.isEmpty()) { //It's a new point
                insertPoint(el);
            } else { //It's an old point
                updatePoint(el);
            }
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
                    if (!neighbourSkyband(el, p, distance) && distance <= R_min) {
                        break;
                    }
                }
            }
        }

        public void updatePoint(LSKYProudData point) {
            //Remove old points from lSky
            point.lsky.replaceAll((key, value) -> value.stream()
                                    .filter((it) -> it.second >= window.start)
                                    .collect(Collectors.toList())
            );

            //Get point lSky
            List<Integer> oldSky = point.lsky.values().stream()
                    .flatMap(Collection::stream)
                    .sorted(Comparator.comparingLong(Tuple<Integer,Long>::getSecond).reversed())
                    .map(Tuple::getFirst)
                    .distinct()
                    .collect(Collectors.toList());

            point.lsky.clear();

            boolean resultFlag = true;

            List<LSKYProudData> indexValuesByArrival = state.index.values().stream()
                    .sorted(Comparator.comparingLong(LSKYProudData::getArrival).reversed())
                    .collect(Collectors.toList());

            for (LSKYProudData data : indexValuesByArrival) {
                boolean innerResultFlag = true;

                if (data.arrival >= window.end - slide) {
                    double distance = Distances.distanceOf(point, data);

                    if (distance <= R_max) {
                        if (!neighbourSkyband(point, data, distance) && distance <= R_min) {
                            resultFlag = false;
                        }
                    }

                } else {
                    innerResultFlag = false;
                }

                if (resultFlag && innerResultFlag)
                    continue;

                break;
            }

            // Check the old skyband elements
            for (Integer id:oldSky) {

                if (!resultFlag)
                    break;

                LSKYProudData p = state.index.get(id);
                double distance = Distances.distanceOf(point, p);

                if (distance <= R_max) {
                    if (!neighbourSkyband(point, p, distance) && distance < R_min) {
                        resultFlag = false;
                    }
                }
            }
        }

        public void deletePoint(LSKYProudData el) {
            state.index.remove(el.id);
        }

        public boolean neighbourSkyband(LSKYProudData el, LSKYProudData neighbour, double distance) {
            int normalizedDistance = normalizeDistance(distance);

            int count = 0;

            for (int i=1; i <= normalizedDistance; i++) {
                count += el.lsky.getOrDefault(i, Lists.make()).size();
            }

            if (count <= K_max - 1) {
                List<Tuple<Integer, Long>> value = el.lsky.getOrDefault(normalizedDistance, new ArrayList<>());
                value.add(new Tuple<>(neighbour.id, neighbour.arrival));

                el.lsky.put(normalizedDistance, value);
                return true;
            }

            return false;
        }

        public int normalizeDistance(double distance) {
            int normalizedDistance = 0;
            int i = 0;

            do {
                if (distance <= R_distinct_list.get(i))
                    normalizedDistance = i + 1;

                i += 1;
            } while (i < R_distinct_list.size() && normalizedDistance == 0);

            return normalizedDistance;
        }

    }

}
