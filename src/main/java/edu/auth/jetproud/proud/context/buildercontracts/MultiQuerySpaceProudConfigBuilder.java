package edu.auth.jetproud.proud.context.buildercontracts;

import java.util.List;

public interface MultiQuerySpaceProudConfigBuilder {
    DatasetProudConfigBuilder querying(List<Integer> kNeighboursDimensions, List<Double> rangeDimensions, int window, int slide);
}
