package edu.auth.jetproud.model;

import java.util.List;

public class AdvancedProudData extends NaiveProudData {

    public AdvancedProudData(AnyProudData point) {
        super(point);
    }

    public AdvancedProudData(int id, List<Double> value, long arrival, int flag) {
        super(id, value, arrival, flag);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AdvancedProudData) {
            return super.equals(obj);
        } else {
            return false;
        }
    }
}
