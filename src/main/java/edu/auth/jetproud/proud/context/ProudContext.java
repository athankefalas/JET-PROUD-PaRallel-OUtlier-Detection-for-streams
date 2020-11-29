package edu.auth.jetproud.proud.context;

import edu.auth.jetproud.application.config.*;

import java.io.Serializable;

public interface ProudContext extends Serializable {

    InputType inputType();
    OutputType outputType();

    String treeInitFileName();

    ProudConfiguration configuration();

    DatasetConfiguration datasetConfiguration();
    KafkaConfiguration kafkaConfiguration();
    InfluxDBConfiguration influxDBConfiguration();

    InternalConfiguration internalConfiguration();

    enum InputType implements Serializable {
        Unknown, File, Kafka
    }

    enum OutputType implements Serializable {
        Unknown, Print, InfluxDB
    }
}
