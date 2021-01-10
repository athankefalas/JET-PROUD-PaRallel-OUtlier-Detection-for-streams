package edu.auth.jetproud.proud.algorithms.contracts;

import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.application.parameters.errors.ProudArgumentException;
import edu.auth.jetproud.exceptions.ProudException;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.model.meta.OutlierQuery;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.algorithms.executors.*;
import edu.auth.jetproud.proud.algorithms.exceptions.NotImplementedAlgorithmException;
import edu.auth.jetproud.proud.partitioning.PartitionedData;
import edu.auth.jetproud.utils.ExceptionUtils;
import edu.auth.jetproud.utils.Tuple;

import java.io.Serializable;

public interface ProudAlgorithmExecutor extends Serializable
{
    ProudAlgorithmOption algorithm();

    void createDistributableData();

    <D extends AnyProudData> StreamStage<Tuple<Long, OutlierQuery>> execute(StreamStage<PartitionedData<D>> streamStage) throws ProudException;


    static ProudAlgorithmExecutor create(ProudContext proudContext) {
        ProudAlgorithmOption algorithmOption = proudContext.configuration().getAlgorithm();

        switch (algorithmOption) {
            case UserDefined:
                throw ExceptionUtils.sneaky(
                        ProudArgumentException.invalid("A User Defined algorithm cannot be auto detected.")
                );
            case Naive:
                return new NaiveProudAlgorithmExecutor(proudContext);
            case Advanced:
                return new AdvancedProudAlgorithmExecutor(proudContext);
            case AdvancedExtended:
                return new AdvancedExtendedProudAlgorithmExecutor(proudContext);
            case Slicing:
                return new SlicingProudAlgorithmExecutor(proudContext);
            case PMCod:
                return new PMCODProudAlgorithmExecutor(proudContext);
            case PMCodNet:
                return new PMCODNetProudAlgorithmExecutor(proudContext);
            case AMCod:
                return new AMCODProudAlgorithmExecutor(proudContext);
            case Sop:
                return new SOPProudAlgorithmExecutor(proudContext);
            case PSod:
                return new PSODProudAlgorithmExecutor(proudContext);
            case PMCSky:
                return new PMCSkyProudAlgorithmExecutor(proudContext);
        }

        throw ExceptionUtils.sneaky(new NotImplementedAlgorithmException(algorithmOption));
    }
}
