package edu.auth.jetproud.proud.context.buildercontracts;

import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;

public interface AlgorithmProudConfigBuilder {

    SpaceProudConfigBuilder forAlgorithm(ProudAlgorithmOption algorithmOption);

}
