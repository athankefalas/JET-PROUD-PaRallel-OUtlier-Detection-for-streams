package edu.auth.jetproud.model;

import java.util.List;

public class AdvancedProudData extends NaiveProudData {

    public AdvancedProudData(AnyProudData point) {
        super(point);
    }

    public AdvancedProudData(int id, List<Double> value, long arrival, int flag) {
        super(id, value, arrival, flag);
    }

    // Copy @See resources/info/ReferenceIssues for issues related to memory alloc & management in java
    public AdvancedProudData copy() {
        AdvancedProudData data = new AdvancedProudData(this);
        data.flag = flag;
        data.count_after.set(count_after.get());
        data.nn_before.addAll(nn_before);
        data.safe_inlier.set(safe_inlier.get());

        return data;
    }

}
