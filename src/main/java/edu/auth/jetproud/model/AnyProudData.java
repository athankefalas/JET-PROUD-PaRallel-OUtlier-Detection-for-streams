package edu.auth.jetproud.model;

import edu.auth.jetproud.model.contracts.EuclideanCoordinate;
import edu.auth.jetproud.utils.EuclideanCoordinateList;
import edu.auth.jetproud.utils.Lists;

import java.io.Serializable;
import java.util.List;

public class AnyProudData implements Serializable, Comparable<AnyProudData>, EuclideanCoordinate
{
    public int id;
    public List<Double> value;
    public int dimensions;
    public long arrival;
    public int flag;
    public List<List<Double>> state;

    public AnyProudData(AnyProudData point) {
        this(point.id, point.value, point.arrival, point.flag);
    }

    public AnyProudData(int id, List<Double> value, long arrival, int flag) {
        this.id = id;
        this.value = value;
        this.dimensions = value.size();
        this.arrival = arrival;
        this.flag = flag;
        this.state = Lists.of(value);
    }

    // Getters

    public int getId() {
        return id;
    }

    public long getArrival() {
        return arrival;
    }

    // EuclideanCoordinate Impl

    public EuclideanCoordinate toEuclideanCoordinate() {
        return new EuclideanCoordinateList<>(value);
    }

    @Override
    public int dimensions() {
        return toEuclideanCoordinate().dimensions();
    }

    @Override
    public double get(int index) {
        return toEuclideanCoordinate().get(index);
    }

    // Hashcode, CompareTo, Equals & toString Implementations

    @Override
    public int hashCode() {
        return state.stream().map(List::hashCode).reduce(0, (a,b)->31 * a + b);
    }

    @Override
    public int compareTo(AnyProudData other) {
        int dimensions = Math.min(this.dimensions, other.dimensions);

        for (int i=0; i < dimensions; i++) {
            if (value.get(i) > other.value.get(i)) {
                return 1;
            } else if (value.get(i) < other.value.get(i)) {
                return -1;
            } else {
                return 0;
            }
        }

        if (this.dimensions > dimensions)
            return 1;
        else
            return -1;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof AnyProudData) {
            AnyProudData otherData = (AnyProudData) other;
            boolean areEqual = id == otherData.id
                    && dimensions == otherData.dimensions
                    && value.equals(otherData.value);

            return areEqual;
        } else {
            return super.equals(other);
        }
    }



    @Override
    public String toString() {
        return "Point(" +
                "id=" + id +
                ", value=" + value +
                ", arrival=" + arrival +
                ", flag=" + flag +
                ")";
    }
}
