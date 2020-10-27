package edu.auth.jetproud.proud.algorithms.contracts;

import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.exceptions.ProudException;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.ProudContext;
import edu.auth.jetproud.proud.algorithms.NaiveProudAlgorithmExecutor;
import edu.auth.jetproud.proud.algorithms.exceptions.NotImplementedAlgorithmException;
import edu.auth.jetproud.proud.partitioning.PartitionedData;
import edu.auth.jetproud.utils.ExceptionUtils;

public interface ProudAlgorithmExecutor
{
    ProudAlgorithmOption algorithm();

    void createDistributableData();

    // TODO: Change Object to StreamStage<?>
    <D extends AnyProudData> Object execute(StreamStage<PartitionedData<D>> streamStage) throws ProudException;


    // TODO: Add implementations
    static ProudAlgorithmExecutor create(ProudContext proudContext) {
        ProudAlgorithmOption algorithmOption = proudContext.getProudConfiguration().getAlgorithm();

        switch (algorithmOption) {
            case Naive:
                return new NaiveProudAlgorithmExecutor(proudContext);
            case Advanced:
                break;
            case AdvancedExtended:
                break;
            case Slicing:
                break;
            case PMCod:
                break;
            case PMCodNet:
                break;
            case AMCod:
                break;
            case Sop:
                break;
            case PSod:
                break;
            case PMCSky:
                break;
        }

        throw ExceptionUtils.sneaky(new NotImplementedAlgorithmException(algorithmOption));
    }
}
