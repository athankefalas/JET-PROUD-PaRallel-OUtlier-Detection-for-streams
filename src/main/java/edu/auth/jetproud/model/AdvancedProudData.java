package edu.auth.jetproud.model;

import java.util.List;

public class AdvancedProudData extends NaiveProudData {

    public AdvancedProudData(AnyProudData point) {
        super(point);
    }

    public AdvancedProudData(int id, List<Double> value, long arrival, int flag) {
        super(id, value, arrival, flag);
    }

}
