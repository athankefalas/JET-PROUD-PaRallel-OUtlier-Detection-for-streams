package edu.auth.jetproud.proud.context;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import edu.auth.jetproud.application.config.*;
import edu.auth.jetproud.application.parameters.ProudConfigurationReader;
import edu.auth.jetproud.application.parameters.errors.ProudArgumentException;
import edu.auth.jetproud.proud.context.buildercontracts.AlgorithmProudConfigBuilder;
import edu.auth.jetproud.proud.context.buildercontracts.DebugSelectionProudConfigBuilder;
import edu.auth.jetproud.proud.partitioning.GridPartitioning;
import edu.auth.jetproud.utils.ExceptionUtils;

public final class Proud implements ProudContext
{
    private String treeInitFileName = "tree_input.txt";

    private InputType inputType = InputType.Unknown;
    private OutputType outputType = OutputType.Unknown;

    private ProudConfiguration proudConfiguration;
    private KafkaConfiguration kafkaConfiguration;
    private InfluxDBConfiguration influxDBConfiguration;
    private ProudInternalConfiguration proudInternalConfiguration;
    private ProudDatasetConfiguration proudDatasetConfiguration;

    private Proud(ProudConfiguration proudConfiguration) {
        this(proudConfiguration,
                KafkaConfiguration.fromSystemEnvironment(),
                InfluxDBConfiguration.fromSystemEnvironment(),
                ProudDatasetConfiguration.fromSystemEnvironment());
    }

    protected Proud(ProudConfiguration proudConfiguration, KafkaConfiguration kafkaConfiguration, InfluxDBConfiguration influxDBConfiguration, ProudDatasetConfiguration proudDatasetConfiguration) {
        this.proudConfiguration = proudConfiguration;
        this.proudDatasetConfiguration = proudDatasetConfiguration;
        this.kafkaConfiguration = kafkaConfiguration;
        this.influxDBConfiguration = influxDBConfiguration;
        this.proudInternalConfiguration = ProudInternalConfiguration.createFrom(proudConfiguration);
    }

    // Getters - ProudContext Impl

    @Override
    public ProudConfiguration configuration() {
        return proudConfiguration;
    }

    @Override
    public KafkaConfiguration kafkaConfiguration() {
        return kafkaConfiguration;
    }

    @Override
    public InfluxDBConfiguration influxDBConfiguration() {
        return influxDBConfiguration;
    }

    @Override
    public ProudInternalConfiguration internalConfiguration() {
        return proudInternalConfiguration;
    }

    @Override
    public ProudDatasetConfiguration datasetConfiguration() {
        return proudDatasetConfiguration;
    }

    // Properties

    @Override
    public InputType inputType() {
        return inputType;
    }

    @Override
    public OutputType outputType() {
        return outputType;
    }

    @Override
    public String treeInitFileName() {
        return treeInitFileName;
    }


    // Methods

    public void execute(Pipeline pipeline) {
        JetInstance jet = Jet.newJetInstance();
        jet.newJob(pipeline).join();
    }

    // Static Init

    public static AlgorithmProudConfigBuilder builder() {
        return new ProudConfigurationBuilder();
    }

    public static DebugSelectionProudConfigBuilder builder(String[] args) throws ProudArgumentException {
        ProudConfigurationReader configReader = new ProudConfigurationReader();
        ProudConfiguration proudConfig = configReader.read(args);

        return new ProudConfigurationBuilder(proudConfig);
    }
}
