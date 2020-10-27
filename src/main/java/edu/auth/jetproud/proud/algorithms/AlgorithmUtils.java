package edu.auth.jetproud.proud.algorithms;

import edu.auth.jetproud.datastructures.vptree.distance.DistanceFunction;
import edu.auth.jetproud.model.AdvancedProudData;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.NaiveProudData;
import edu.auth.jetproud.model.contracts.EuclideanCoordinate;
import edu.auth.jetproud.utils.Utils;

import java.util.Objects;

public final class AlgorithmUtils
{

    private AlgorithmUtils(){}

    public static <D extends AnyProudData> Double distanceOf(D one, D other) {
        DistanceFunction<D> distance = DistanceFunction.euclidean();
        return distance.getDistance(one, other);
    }

    public static Double distanceOf(EuclideanCoordinate one, EuclideanCoordinate other) {
        DistanceFunction<EuclideanCoordinate> distance = DistanceFunction.euclidean();
        return distance.getDistance(one, other);
    }

    public static NaiveProudData combineElements(NaiveProudData one, NaiveProudData other, int k) {
        if (one == null || other == null) {
            return Utils.firstNonNull(one, other);
        }

        other.nn_before.forEach((it)->one.insert_nn_before(it, k));
        return one;
    }

    public static AdvancedProudData combineNewElements(AdvancedProudData one, AdvancedProudData other, int k) {
        if (one == null || other == null) {
            return Utils.firstNonNull(one, other);
        }

        if (one.flag == other.flag && one.flag == 1) {
            other.nn_before.forEach((it)->one.insert_nn_before(it, k));
            return one;
        } else if (other.flag == 0) {
            one.nn_before.forEach((it)->other.insert_nn_before(it, k));
            return other;
        } else if (one.flag == 0) {
            other.nn_before.forEach((it)->one.insert_nn_before(it, k));
            return one;
        }

        return null;
    }

    public static AdvancedProudData combineOldElements(AdvancedProudData one, AdvancedProudData other, int k) {
        if (one == null || other == null) {
            return Utils.firstNonNull(one, other);
        }

        one.count_after = other.count_after;
        one.safe_inlier = other.safe_inlier;

        return one;
    }


}
