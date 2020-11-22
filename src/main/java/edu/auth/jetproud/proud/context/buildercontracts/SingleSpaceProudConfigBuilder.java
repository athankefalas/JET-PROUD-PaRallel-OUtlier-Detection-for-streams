package edu.auth.jetproud.proud.context.buildercontracts;

import edu.auth.jetproud.proud.context.buildercontracts.DatasetProudConfigBuilder;

public interface SingleSpaceProudConfigBuilder {
    DatasetProudConfigBuilder querying(int kNeighbours, double range, int window, int slide);
}
