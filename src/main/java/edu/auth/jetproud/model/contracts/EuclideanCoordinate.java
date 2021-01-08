package edu.auth.jetproud.model.contracts;

import edu.auth.jetproud.datastructures.mtree.MTreeInsertable;

/**
 * An interface to represent coordinates in Euclidean spaces.
 * @see <a href="http://en.wikipedia.org/wiki/Euclidean_space">"Euclidean
 *      Space" article at Wikipedia</a>
 */
public interface EuclideanCoordinate {
    /**
     * The number of dimensions.
     */
    int dimensions();

    /**
     * A method to access the {@code index}-th component of the coordinate.
     *
     * @param index The index of the component. Must be less than {@link
     *              #dimensions()}.
     */
    double get(int index);
}
