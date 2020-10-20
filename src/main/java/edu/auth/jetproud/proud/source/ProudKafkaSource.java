package edu.auth.jetproud.proud.source;

import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.StreamSource;
import edu.auth.jetproud.application.config.KafkaConfiguration;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.ProudContext;
import edu.auth.jetproud.utils.Parser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class ProudKafkaSource<T extends AnyProudData> implements ProudSource<T> {

    private ProudContext proudContext;
    private Parser<T> parser;

    public ProudKafkaSource(ProudContext proudContext, Parser<T> parser) {
        this.proudContext = proudContext;
        this.parser = parser;
    }

    private <K,V> T projectionFunction(ConsumerRecord<K,V> record) {
        // TODO check the type of value and add actual parsing !!!!
        Object value = record.value();
        return null;
    }

    @Override
    public StreamSource<T> createJetSource() {
        KafkaConfiguration kafkaConfig = proudContext.getKafkaConfiguration();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaConfig.getKafkaBrokers());
        props.setProperty("key.deserializer", StringDeserializer.class.getCanonicalName());
        props.setProperty("value.deserializer", StringDeserializer.class.getCanonicalName());
        props.setProperty("auto.offset.reset", "earliest");

        return KafkaSources.kafka(props, this::projectionFunction,kafkaConfig.getKafkaTopic());
    }
}
