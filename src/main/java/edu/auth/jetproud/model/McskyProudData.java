package edu.auth.jetproud.model;

import edu.auth.jetproud.utils.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class McskyProudData extends AnyProudData
{

    //Neighbor data
    public Map<Integer, List<Tuple<Integer, Long>>> lsky;
    //Skip flag
    public boolean safe_inlier;
    //Micro-cluster data
    public int mc;

    public McskyProudData(AnyProudData point) {
        super(point);
        postInit();
    }

    public McskyProudData(int id, List<Double> value, long arrival, int flag) {
        super(id, value, arrival, flag);
        postInit();
    }

    private void postInit() {
        lsky = new HashMap<>();
        safe_inlier = false;
        mc = -1;
    }

    public void clear(int newMc) {
        lsky.clear();
        mc = newMc;
    }

}
