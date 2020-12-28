package edu.auth.jetproud.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SlicingProudData extends AnyProudData
{

    //Neighbor data
    public AtomicInteger count_after;
    public ConcurrentHashMap<Long, Integer> slices_before;
    //Skip flag
    public AtomicBoolean safe_inlier;
    //Slice check
    public AtomicLong last_check;

    public SlicingProudData(AnyProudData point) {
        super(point);
        postInit();
    }

    public SlicingProudData(int id, List<Double> value, long arrival, int flag) {
        super(id, value, arrival, flag);
        postInit();
    }

    private void postInit() {
        count_after = new AtomicInteger(0);
        slices_before = new ConcurrentHashMap<>();
        safe_inlier = new AtomicBoolean(false);
        last_check = new AtomicLong(0L);
    }
}
