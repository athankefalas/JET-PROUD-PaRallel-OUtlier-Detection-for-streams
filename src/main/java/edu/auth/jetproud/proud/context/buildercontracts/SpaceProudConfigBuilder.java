package edu.auth.jetproud.proud.context.buildercontracts;

public interface SpaceProudConfigBuilder {

    SingleSpaceProudConfigBuilder inSingleSpace();

    MultiQuerySpaceProudConfigBuilder inMultiQuerySpace();

    MultiQueryMultiWindowSpaceProudConfigBuilder inMultiQueryMultiWindowSpace();

}
