package edu.auth.jetproud.proud.context;

import edu.auth.jetproud.application.config.InfluxDBConfiguration;
import edu.auth.jetproud.application.config.KafkaConfiguration;
import edu.auth.jetproud.application.config.ProudConfiguration;
import edu.auth.jetproud.application.config.DatasetConfiguration;
import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.application.parameters.data.ProudPartitioningOption;
import edu.auth.jetproud.application.parameters.data.ProudSpaceOption;
import edu.auth.jetproud.application.parameters.errors.ProudArgumentException;
import edu.auth.jetproud.proud.context.buildercontracts.*;
import edu.auth.jetproud.proud.partitioning.GridPartitioning;
import edu.auth.jetproud.utils.ExceptionUtils;
import edu.auth.jetproud.utils.Lists;

import java.util.List;

public class ProudConfigurationBuilder
        implements AlgorithmProudConfigBuilder,
        SpaceProudConfigBuilder,
        SingleSpaceProudConfigBuilder, MultiQuerySpaceProudConfigBuilder, MultiQueryMultiWindowSpaceProudConfigBuilder,
        DatasetProudConfigBuilder,
        DatasetInputProudConfigBuilder,
        DatasetKafkaInputProudConfigBuilder,
        PartitioningProudConfigBuilder,
        OutputSelectionProudConfigBuilder,
        InfluxDBDatabaseProudConfigBuilder, InfluxDBHostProudConfigBuilder, InfluxDBHostAuthenticationProudConfigBuilder,
        DebugSelectionProudConfigBuilder,
        BuildableProudConfigBuilder

{

    private String treeInitFileName = "tree_input.txt";

    private ProudContext.InputType inputType = ProudContext.InputType.Unknown;
    private ProudContext.OutputType outputType = ProudContext.OutputType.Unknown;

    private ProudConfiguration proudConfiguration;
    private DatasetConfiguration datasetConfiguration;

    private KafkaConfiguration kafkaConfiguration;
    private InfluxDBConfiguration influxDBConfiguration;

    protected ProudConfigurationBuilder() {
        proudConfiguration = new ProudConfiguration();
        datasetConfiguration = DatasetConfiguration.fromSystemEnvironment();
        kafkaConfiguration = KafkaConfiguration.fromSystemEnvironment();
        influxDBConfiguration = InfluxDBConfiguration.fromSystemEnvironment();
    }

    protected ProudConfigurationBuilder(ProudConfiguration proudConfiguration) {
        this.proudConfiguration = proudConfiguration;
        datasetConfiguration = DatasetConfiguration.fromSystemEnvironment();
        kafkaConfiguration = KafkaConfiguration.fromSystemEnvironment();
        influxDBConfiguration = InfluxDBConfiguration.fromSystemEnvironment();
    }

    // Algorithm

    @Override
    public SpaceProudConfigBuilder forAlgorithm(ProudAlgorithmOption algorithmOption) {
        proudConfiguration.setAlgorithm(algorithmOption);
        return this;
    }

    // Space

    @Override
    public SingleSpaceProudConfigBuilder inSingleSpace() {
        proudConfiguration.setSpace(ProudSpaceOption.Single);
        return this;
    }

    @Override
    public MultiQuerySpaceProudConfigBuilder inMultiQuerySpace() {
        proudConfiguration.setSpace(ProudSpaceOption.MultiQueryMultiParams);
        return this;
    }

    @Override
    public MultiQueryMultiWindowSpaceProudConfigBuilder inMultiQueryMultiWindowSpace() {
        proudConfiguration.setSpace(ProudSpaceOption.MultiQueryMultiParamsMultiWindowParams);
        return this;
    }

    // Single Space Query

    @Override
    public DatasetProudConfigBuilder querying(int kNeighbours, double range, int window, int slide) {
        proudConfiguration.setKNeighbours(Lists.of(kNeighbours));
        proudConfiguration.setRNeighbourhood(Lists.of(range));
        proudConfiguration.setWindowSizes(Lists.of(window));
        proudConfiguration.setSlideSizes(Lists.of(slide));

        return this;
    }


    // Multi Query Space Query

    @Override
    public DatasetProudConfigBuilder querying(List<Integer> kNeighboursDimensions, List<Double> rangeDimensions, int window, int slide) {
        proudConfiguration.setKNeighbours(Lists.copyOf(kNeighboursDimensions));
        proudConfiguration.setRNeighbourhood(Lists.copyOf(rangeDimensions));
        proudConfiguration.setWindowSizes(Lists.of(window));
        proudConfiguration.setSlideSizes(Lists.of(slide));

        return this;
    }


    // Multi Query - Window Space Query

    @Override
    public DatasetProudConfigBuilder querying(List<Integer> kNeighboursDimensions, List<Double> rangeDimensions, List<Integer> windowDimensions, List<Integer> slideDimensions) {
        proudConfiguration.setKNeighbours(Lists.copyOf(kNeighboursDimensions));
        proudConfiguration.setRNeighbourhood(Lists.copyOf(rangeDimensions));
        proudConfiguration.setWindowSizes(Lists.copyOf(windowDimensions));
        proudConfiguration.setSlideSizes(Lists.copyOf(slideDimensions));

        return this;
    }

    // Dataset Name

    @Override
    public DatasetInputProudConfigBuilder forDatasetNamed(String name) {
        proudConfiguration.setDataset(name);
        return this;
    }

    // Dataset Input

    @Override
    public PartitioningProudConfigBuilder locatedIn(String directory) {
        datasetConfiguration.setDatasetHome(directory);
        inputType = ProudContext.InputType.File;
        return this;
    }

    @Override
    public DatasetKafkaInputProudConfigBuilder fromKafka() {
        inputType = ProudContext.InputType.Kafka;
        return this;
    }

    // Dataset - Kafka Topic

    @Override
    public PartitioningProudConfigBuilder inTopic(String topic, String brokers) {
        kafkaConfiguration.setKafkaTopic(topic);
        kafkaConfiguration.setKafkaBrokers(brokers);
        return this;
    }

    // Partitioning


    @Override
    public OutputSelectionProudConfigBuilder userDefinedPartitioning() {
        proudConfiguration.setPartitioning(ProudPartitioningOption.UserDefined);
        return this;
    }

    @Override
    public OutputSelectionProudConfigBuilder replicationPartitioned() {
        proudConfiguration.setPartitioning(ProudPartitioningOption.Replication);
        return this;
    }

    @Override
    public OutputSelectionProudConfigBuilder treePartitionedUsing(String initialNodeFile, int initialNodeCount) {
        proudConfiguration.setPartitioning(ProudPartitioningOption.Tree);
        proudConfiguration.setTreeInitialNodeCount(initialNodeCount);
        this.treeInitFileName = initialNodeFile;
        return this;
    }

    @Override
    public OutputSelectionProudConfigBuilder gridPartitionedUsing(GridPartitioning.GridPartitioner gridPartitioner) {
        proudConfiguration.setPartitioning(ProudPartitioningOption.Grid);
        proudConfiguration.setCustomGridPartitioner(gridPartitioner);
        return this;
    }


    // Output

    @Override
    public DebugSelectionProudConfigBuilder printingOutliers() {
        outputType = ProudContext.OutputType.Print;
        return this;
    }

    @Override
    public InfluxDBDatabaseProudConfigBuilder writingOutliersToInfluxDB() {
        outputType = ProudContext.OutputType.InfluxDB;
        return this;
    }

    // Influx DB

    @Override
    public InfluxDBHostProudConfigBuilder inDatabase(String dbName) {
        influxDBConfiguration.setDatabase(dbName);
        return this;
    }

    @Override
    public InfluxDBHostAuthenticationProudConfigBuilder locatedAt(String host) {
        influxDBConfiguration.setHost(host);
        return this;
    }

    @Override
    public DebugSelectionProudConfigBuilder authenticatedWith(String user, String password) {
        influxDBConfiguration.setUser(user);
        influxDBConfiguration.setPassword(password);
        return this;
    }


    // Debug & Build

    @Override
    public BuildableProudConfigBuilder enablingDebug() {
        proudConfiguration.setDebug(true);
        return this;
    }

    // Build

    @Override
    public Proud build() {
        Proud proud = new Proud(proudConfiguration, kafkaConfiguration, influxDBConfiguration, datasetConfiguration);
        proud.setInputType(inputType);
        proud.setOutputType(outputType);
        proud.setTreeInitFileName(treeInitFileName);

        try {
            proud.configuration().validateConfiguration();
        } catch (ProudArgumentException e) {
            throw ExceptionUtils.sneaky(e);
        }

        return proud;
    }

}


// Builder Stage Interfaces

