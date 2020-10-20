package edu.auth.jetproud.proud;

import edu.auth.jetproud.application.config.*;

import java.io.Serializable;

public interface ProudContext extends Serializable {

    void setGlobalProperty(String name, String value);
    String getGlobalProperty(String name);

    ProudConfiguration getProudConfiguration();

    ProudDatasetResourceConfiguration getProudDatasetResourceConfiguration();
    KafkaConfiguration getKafkaConfiguration();
    InfluxDBConfiguration getInfluxDBConfiguration();

    ProudInternalConfiguration getProudInternalConfiguration();



}
