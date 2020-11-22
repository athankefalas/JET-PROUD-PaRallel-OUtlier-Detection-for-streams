package edu.auth.jetproud.proud.context.buildercontracts;

import java.util.List;

public interface MultiQueryMultiWindowSpaceProudConfigBuilder {
    DatasetProudConfigBuilder querying(List<Integer> kNeighboursDimensions, List<Double> rangeDimensions, List<Integer> windowDimensions, List<Integer> slideDimensions);
}
