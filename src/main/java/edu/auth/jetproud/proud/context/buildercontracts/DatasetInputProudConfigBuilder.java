package edu.auth.jetproud.proud.context.buildercontracts;

public interface DatasetInputProudConfigBuilder {
    PartitioningProudConfigBuilder locatedIn(String directory);

    DatasetKafkaInputProudConfigBuilder fromKafka();
}
