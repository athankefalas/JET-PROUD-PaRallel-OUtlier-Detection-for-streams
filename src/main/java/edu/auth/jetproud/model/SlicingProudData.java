package edu.auth.jetproud.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SlicingProudData extends AnyProudData
{

    //Neighbor data
    public int count_after;
    public Map<Long, Integer> slices_before;
    //Skip flag
    public boolean safe_inlier;
    //Slice check
    public long last_check;

    public SlicingProudData(AnyProudData point) {
        super(point);
        postInit();
    }

    public SlicingProudData(int id, List<Double> value, long arrival, int flag) {
        super(id, value, arrival, flag);
        postInit();
    }

    private void postInit() {
        count_after = 0;
        slices_before = new HashMap<>();
        safe_inlier = false;
        last_check = 0;
    }
}
