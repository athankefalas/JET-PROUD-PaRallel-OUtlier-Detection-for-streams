package edu.auth.jetproud.application.config;

public class KafkaConfiguration
{

    public static final String KAFKA_TOPIC_PROPERTY = "KAFKA_TOPIC";
    public static final String KAFKA_BROKERS_PROPERTY = "KAFKA_BROKERS";

    private String kafkaTopic;
    private String kafkaBrokers;

    private KafkaConfiguration() {
        this("","");
    }

    public KafkaConfiguration(String kafkaTopic, String kafkaBrokers) {
        this.kafkaTopic = kafkaTopic;
        this.kafkaBrokers = kafkaBrokers;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public String getKafkaBrokers() {
        return kafkaBrokers;
    }

    public void setKafkaBrokers(String kafkaBrokers) {
        this.kafkaBrokers = kafkaBrokers;
    }

    public static KafkaConfiguration fromSystemEnvironment() {
        KafkaConfiguration config = new KafkaConfiguration();
        config.kafkaTopic = System.getenv(KAFKA_TOPIC_PROPERTY);
        config.kafkaBrokers = System.getenv(KAFKA_BROKERS_PROPERTY);

        return config;
    }

    public static KafkaConfiguration fromSystemProperties() {
        KafkaConfiguration config = new KafkaConfiguration();
        config.kafkaTopic = System.getProperty(KAFKA_TOPIC_PROPERTY);
        config.kafkaBrokers = System.getProperty(KAFKA_BROKERS_PROPERTY);

        return config;
    }


}
