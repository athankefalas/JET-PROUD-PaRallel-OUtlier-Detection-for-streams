package edu.auth.jetproud.proud.source.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.nonapi.io.github.classgraph.json.JSONDeserializer;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamStage;
import edu.auth.jetproud.application.config.KafkaConfiguration;
import edu.auth.jetproud.model.AnyProudData;
import edu.auth.jetproud.proud.context.ProudContext;
import edu.auth.jetproud.proud.source.ProudSource;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Parser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

public class ProudKafkaSource<T extends AnyProudData> implements ProudSource<T> {

    private ProudContext proudContext;
    private Function<AnyProudData,T> converter;

    public ProudKafkaSource(ProudContext proudContext, Function<AnyProudData,T> converter) {
        this.proudContext = proudContext;
        this.converter = converter;
    }

    private T projectionFunction(ConsumerRecord<String, JsonNode> record) {
        // Conversion from kafka record to T
        int id = record.value().get("id").asInt();
        long timestamp = record.value().get("timestamp").asLong();

        List<Double> spatialValues = Lists.make();

        for (Iterator<JsonNode> it = record.value().get("value").elements(); it.hasNext();) {
            JsonNode node = it.next();

            if (node == null)
                break;

            spatialValues.add(node.doubleValue());
        }

        AnyProudData data = new AnyProudData(id, spatialValues, timestamp, 0);
        return converter.apply(data);
    }

    @Override
    public StreamStage<T> readInto(Pipeline pipeline) {
        long allowedLag = proudContext.internalConfiguration().getAllowedLateness();

        return pipeline.readFrom(createJetSource())
                .withTimestamps((it)->it.arrival, allowedLag);
    }

    private StreamSource<T> createJetSource() {
        KafkaConfiguration kafkaConfig = proudContext.kafkaConfiguration();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaConfig.getKafkaBrokers());
        props.setProperty("key.deserializer", StringDeserializer.class.getCanonicalName());
        props.setProperty("value.deserializer", JSONDeserializer.class.getCanonicalName());
        props.setProperty("auto.offset.reset", "earliest");

        return KafkaSources.kafka(props, this::projectionFunction, kafkaConfig.getKafkaTopic());
    }
}
