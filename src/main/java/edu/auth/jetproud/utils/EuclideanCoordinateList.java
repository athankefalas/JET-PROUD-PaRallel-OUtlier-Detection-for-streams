package edu.auth.jetproud.utils;

import edu.auth.jetproud.model.contracts.EuclideanCoordinate;

import java.util.List;

public class EuclideanCoordinateList<E extends Number> implements EuclideanCoordinate {

    private List<E> delegatingList;

    private EuclideanCoordinateList() {
        this(null);
    }

    public EuclideanCoordinateList(List<E> list) {
        this.delegatingList = list != null ? list : Lists.make();
    }

    @Override
    public int dimensions() {
        return delegatingList.size();
    }

    @Override
    public double get(int index) {
        return delegatingList.get(index).doubleValue();
    }
}
