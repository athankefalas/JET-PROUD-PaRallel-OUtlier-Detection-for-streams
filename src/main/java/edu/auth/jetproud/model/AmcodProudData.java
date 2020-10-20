package edu.auth.jetproud.model;

import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AmcodProudData extends AnyProudData
{

    //Neighbor data
    public int count_after;
    public List<Tuple<Long, Double>> nn_before_set;
    public List<Double> count_after_set;

    //Skip flag
    public boolean safe_inlier;

    //Micro-cluster data
    public int mc;
    public Set<Integer> Rmc;

    public AmcodProudData(AnyProudData point) {
        super(point);
        postInit();
    }

    public AmcodProudData(int id, List<Double> value, long arrival, int flag) {
        super(id, value, arrival, flag);
        postInit();
    }

    private void postInit() {
        count_after = 0;
        nn_before_set = Lists.make();
        count_after_set = Lists.make();

        safe_inlier = false;

        mc = -1;
        Rmc = new HashSet<>();
    }

    // Implementation
    public void clear(int newMc) {
        nn_before_set.clear();
        count_after_set.clear();
        count_after = 0;
        mc = newMc;
    }

}
