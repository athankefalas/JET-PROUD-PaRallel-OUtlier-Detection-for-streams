package edu.auth.jetproud.model.meta;

import edu.auth.jetproud.model.AnyProudData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OutlierMetadata<T extends AnyProudData>
{
    private Map<Integer, T> outliers;

    public OutlierMetadata() {
        outliers = new HashMap<>();
    }

    public OutlierMetadata(Map<Integer, T> outliers) {
        this.outliers = new HashMap<>(outliers);
    }

    public Map<Integer, T> getOutliers() {
        return outliers;
    }

    public void addOutliers(Map<Integer, T> outliers) {
        this.outliers.putAll(outliers);
    }

    public void setOutliers(Map<Integer, T> outliers) {
        this.outliers = outliers;
    }

    public OutlierMetadata<T> copy() {
        return new OutlierMetadata<>(outliers);
    }
}
