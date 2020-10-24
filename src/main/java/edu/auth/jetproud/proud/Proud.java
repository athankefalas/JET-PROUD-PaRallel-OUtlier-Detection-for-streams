package edu.auth.jetproud.proud;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import edu.auth.jetproud.application.config.*;
import edu.auth.jetproud.application.parameters.ProudConfigurationReader;
import edu.auth.jetproud.application.parameters.errors.ProudArgumentException;

import java.util.HashMap;
import java.util.Map;

public final class Proud implements ProudContext
{

    private final Map<String, String> properties = new HashMap<>();

    private ProudConfiguration proudConfiguration;
    private KafkaConfiguration kafkaConfiguration;
    private InfluxDBConfiguration influxDBConfiguration;
    private ProudInternalConfiguration proudInternalConfiguration;
    private ProudDatasetResourceConfiguration proudDatasetResourceConfiguration;

    private Proud(ProudConfiguration proudConfiguration) {
        this(proudConfiguration,
                KafkaConfiguration.fromSystemEnvironment(),
                InfluxDBConfiguration.fromSystemEnvironment(),
                ProudDatasetResourceConfiguration.fromSystemEnvironment());
    }

    private Proud(ProudConfiguration proudConfiguration, KafkaConfiguration kafkaConfiguration, InfluxDBConfiguration influxDBConfiguration, ProudDatasetResourceConfiguration proudDatasetResourceConfiguration) {
        this.proudConfiguration = proudConfiguration;
        this.kafkaConfiguration = kafkaConfiguration;
        this.influxDBConfiguration = influxDBConfiguration;
        this.proudInternalConfiguration = ProudInternalConfiguration.createFrom(proudConfiguration);
        this.proudDatasetResourceConfiguration = proudDatasetResourceConfiguration;
    }

    // Getters - ProudContext Impl

    @Override
    public ProudConfiguration getProudConfiguration() {
        return proudConfiguration;
    }

    @Override
    public KafkaConfiguration getKafkaConfiguration() {
        return kafkaConfiguration;
    }

    @Override
    public InfluxDBConfiguration getInfluxDBConfiguration() {
        return influxDBConfiguration;
    }

    @Override
    public ProudInternalConfiguration getProudInternalConfiguration() {
        return proudInternalConfiguration;
    }

    @Override
    public ProudDatasetResourceConfiguration getProudDatasetResourceConfiguration() {
        return proudDatasetResourceConfiguration;
    }

    // Properties

    @Override
    public void setGlobalProperty(String name, String value) {
        properties.put(name, value);
    }

    @Override
    public String getGlobalProperty(String name) {
        return properties.get(name);
    }

    // Methods

    public void execute(Pipeline pipeline) {
        JetInstance jet = Jet.newJetInstance();
        jet.newJob(pipeline).join();
    }

    // Static Init

    public static Builder builder() {
        return new Builder(null);
    }

    public static Builder builder(String[] args) throws ProudArgumentException {
        ProudConfigurationReader configReader = new ProudConfigurationReader();
        ProudConfiguration proudConfig = configReader.read(args);

        return new Builder(proudConfig);
    }

    public static Builder builder(ProudConfiguration proudConfiguration) {
        return new Builder(proudConfiguration);
    }

    // Builder

    public static class Builder
    {
        private final Proud proud;

        private Builder(ProudConfiguration configuration) {
            proud = new Proud(configuration);
        }

        public Builder withTerminalArguments(String[] args) throws ProudArgumentException {
            ProudConfigurationReader configReader = new ProudConfigurationReader();
            ProudConfiguration proudConfig = configReader.read(args);

            return withConfiguration(proudConfig);
        }

        public Builder withConfiguration(ProudConfiguration proudConfiguration) {
            proud.proudConfiguration = proudConfiguration;
            return this;
        }

        public Builder withDatasetResourceConfiguration(ProudDatasetResourceConfiguration datasetResourceConfiguration) {
            proud.proudDatasetResourceConfiguration = datasetResourceConfiguration;
            return this;
        }

        public Builder withKafkaConfiguration(KafkaConfiguration kafkaConfiguration) {
            proud.kafkaConfiguration = kafkaConfiguration;
            return this;
        }

        public Builder withInfluxDBConfiguration(InfluxDBConfiguration influxDBConfiguration) {
            proud.influxDBConfiguration = influxDBConfiguration;
            return this;
        }

        public Builder withInternalConfiguration(ProudInternalConfiguration proudInternalConfiguration) {
            proud.proudInternalConfiguration = proudInternalConfiguration;
            return this;
        }

        public Proud build() {
            return proud;
        }
    }

}
