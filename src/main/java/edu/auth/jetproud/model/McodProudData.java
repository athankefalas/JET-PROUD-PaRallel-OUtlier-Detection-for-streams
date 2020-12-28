package edu.auth.jetproud.model;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class McodProudData extends NaiveProudData
{

    //Micro-cluster data
    public int mc;
    public CopyOnWriteArraySet<Integer> Rmc;

    public McodProudData(AnyProudData point) {
        super(point);
        postInit();
    }

    public McodProudData(int id, List<Double> value, long arrival, int flag) {
        super(id, value, arrival, flag);
        postInit();
    }

    private void postInit() {
        mc = -1;
        Rmc = new CopyOnWriteArraySet<>();
    }

    @Override
    public void clear(int newMc) {
        super.clear(newMc);
        mc = newMc;
    }
}
