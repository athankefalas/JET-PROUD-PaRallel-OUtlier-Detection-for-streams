package edu.auth.jetproud.datastructures.mtree;

/**
 * The type of the results for nearest-neighbor queries.
 */
public class ResultItem<DATA> {

    protected ResultItem(DATA data, double distance) {
        this.data = data;
        this.distance = distance;
    }

    /**
     * A nearest-neighbor.
     */
    public DATA data;

    /**
     * The distance from the nearest-neighbor to the query data object
     * parameter.
     */
    public double distance;
}
