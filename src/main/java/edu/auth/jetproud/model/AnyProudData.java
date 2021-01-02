package edu.auth.jetproud.model;

import edu.auth.jetproud.datastructures.mtree.MTreeInsertable;
import edu.auth.jetproud.model.contracts.EuclideanCoordinate;
import edu.auth.jetproud.utils.EuclideanCoordinateList;
import edu.auth.jetproud.utils.Lists;

import java.io.Serializable;
import java.util.List;

public class AnyProudData implements Serializable, Comparable<AnyProudData>, EuclideanCoordinate, MTreeInsertable
{
    public int id;
    public List<Double> value;
    public int dimensions;
    public long arrival;
    public int flag;

    public AnyProudData(AnyProudData point) {
        this(point.id, point.value, point.arrival, point.flag);
    }

    public AnyProudData(int id, List<Double> value, long arrival, int flag) {
        this.id = id;
        this.value = Lists.copyOf(value);
        this.dimensions = value.size();
        this.arrival = arrival;
        this.flag = flag;
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
    public long spacialIdentity() {
        return value.hashCode();
    }

    @Override
    public int hashCode() {
        // Auto-generated hashcode, this locates the proper object in maps and other DS that
        // rely on hashcode identity to identify and add / remove objects
        int result = id;
        result = 31 * result + value.hashCode();
        result = 31 * result + dimensions;
        result = 31 * result + (int) (arrival ^ (arrival >>> 32));
        result = 31 * result + flag;
        return result;

        // Use list hashcode instead, it's the same thing.
        //return Lists.of(value).stream().map(List::hashCode).reduce(0, (a,b)->31 * a + b);
        //return value.hashCode();
    }

    @Override
    public int compareTo(AnyProudData other) {
        int dimensions = Math.min(this.dimensions, other.dimensions);

        if (dimensions >= 1) {
            return value.get(0).compareTo(other.value.get(0));
        }

        return Integer.compare(this.dimensions, dimensions);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof AnyProudData) {
            AnyProudData otherData = (AnyProudData) other;
            return id == otherData.id
                    && dimensions == otherData.dimensions
                    && spacialIdentity() == otherData.spacialIdentity();
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
