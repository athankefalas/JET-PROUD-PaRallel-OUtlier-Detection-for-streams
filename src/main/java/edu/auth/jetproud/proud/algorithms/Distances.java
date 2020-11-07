package edu.auth.jetproud.proud.algorithms;

import edu.auth.jetproud.datastructures.vptree.distance.DistanceFunction;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.contracts.EuclideanCoordinate;

public final class Distances
{

    private Distances(){}

    public static <D extends AnyProudData> Double distanceOf(D one, D other) {
        DistanceFunction<D> distance = DistanceFunction.euclidean();
        return distance.getDistance(one, other);
    }

    public static Double distanceOf(EuclideanCoordinate one, EuclideanCoordinate other) {
        DistanceFunction<EuclideanCoordinate> distance = DistanceFunction.euclidean();
        return distance.getDistance(one, other);
    }

}
