package edu.auth.jetproud.proud.algorithms.executors.custom;

import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.algorithms.AnyProudAlgorithmExecutor;
import edu.auth.jetproud.proud.context.ProudContext;

public abstract class UserDefinedProudAlgorithmExecutor<D extends AnyProudData> extends AnyProudAlgorithmExecutor<D>
{
    public UserDefinedProudAlgorithmExecutor(ProudContext proudContext) {
        super(proudContext, ProudAlgorithmOption.UserDefined);
    }

}
