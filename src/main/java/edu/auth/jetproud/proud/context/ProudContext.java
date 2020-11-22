package edu.auth.jetproud.proud.context;

import edu.auth.jetproud.application.config.*;

import java.io.Serializable;

public interface ProudContext extends Serializable {

    InputType inputType();
    OutputType outputType();

    String treeInitFileName();

    ProudConfiguration configuration();

    ProudDatasetConfiguration datasetConfiguration();
    KafkaConfiguration kafkaConfiguration();
    InfluxDBConfiguration influxDBConfiguration();

    ProudInternalConfiguration internalConfiguration();

    enum InputType {
        Unknown, File, Kafka
    }

    enum OutputType {
        Unknown, Print, InfluxDB
    }
}
