package edu.auth.jetproud.proud.context.buildercontracts;

public interface DatasetKafkaInputProudConfigBuilder {
    PartitioningProudConfigBuilder inTopic(String topic, String brokers);
}
