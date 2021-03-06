package edu.auth.jetproud.model;

import edu.auth.jetproud.utils.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LSKYProudData extends AnyProudData
{
    //Neighbor data
    public ConcurrentHashMap<Integer, List<Tuple<Integer, Long>>> lsky;

    //Skip flag
    public boolean safe_inlier;

    public LSKYProudData(AnyProudData point) {
        super(point);
        postInit();
    }

    public LSKYProudData(int id, List<Double> value, long arrival, int flag) {
        super(id, value, arrival, flag);
        postInit();
    }

    private void postInit() {
        lsky = new ConcurrentHashMap<>();
        safe_inlier = false;
    }

    // Clear variables
    public void clear() {
        lsky.clear();
    }
}
