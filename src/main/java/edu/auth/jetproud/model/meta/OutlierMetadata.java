package edu.auth.jetproud.model.meta;

import edu.auth.jetproud.model.AnyProudData;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OutlierMetadata<T extends AnyProudData> implements Serializable
{
    private final ConcurrentHashMap<Integer, T> outliers;

    public OutlierMetadata() {
        outliers = new ConcurrentHashMap<>();
    }

    public OutlierMetadata(Map<Integer, T> outliers) {
        this.outliers = new ConcurrentHashMap<>(outliers);
    }

    public Map<Integer, T> getOutliers() {
        return outliers;
    }

    public OutlierMetadata<T> copy() {
        return new OutlierMetadata<>(outliers);
    }
}
