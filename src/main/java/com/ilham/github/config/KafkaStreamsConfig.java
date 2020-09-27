package com.ilham.github.config;

import com.ilham.github.avro.*;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.HashMap;
import java.util.Properties;

public class KafkaStreamsConfig {

    public static Properties getProducerProperties() {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url","http://127.0.0.1:8085");

        return properties;
    }

    public static Properties getKafkaStreamsProperties() {
        Properties properties = new Properties();

        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"kafka-streams-examples");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        properties.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,"100");
        properties.setProperty("schema.registry.url","http://127.0.0.1:8085");

        return properties;
    }

    public static SpecificAvroSerde<Author> getAuthorAvroSerde() {
        SpecificAvroSerde<Author> authorAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url","http://127.0.0.1:8085");

        authorAvroSerde.configure(serdeConfig, false);
        return authorAvroSerde;
    }

    public static SpecificAvroSerde<Book> getBookAvroSerde() {
        SpecificAvroSerde<Book> bookAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url","http://127.0.0.1:8085");

        bookAvroSerde.configure(serdeConfig, false);
        return bookAvroSerde;
    }

    public static SpecificAvroSerde<Customer> getCustomerAvroSerde() {
        SpecificAvroSerde<Customer> customerAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url","http://127.0.0.1:8085");

        customerAvroSerde.configure(serdeConfig, false);
        return customerAvroSerde;
    }

    public static SpecificAvroSerde<Transaction> getTransactionAvroSerde() {
        SpecificAvroSerde<Transaction> transactionAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url","http://127.0.0.1:8085");

        transactionAvroSerde.configure(serdeConfig, false);
        return transactionAvroSerde;
    }

    public static SpecificAvroSerde<EnrichedBook> getEnrichedBookAvroSerde() {
        SpecificAvroSerde<EnrichedBook> enrichedBookAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url","http://127.0.0.1:8085");

        enrichedBookAvroSerde.configure(serdeConfig, false);
        return enrichedBookAvroSerde;
    }

    public static SpecificAvroSerde<EnrichedTransaction> getEnrichedTransactionAvroSerde() {
        SpecificAvroSerde<EnrichedTransaction> enrichedTransactionAvroSerde = new SpecificAvroSerde<>();

        final HashMap<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url","http://127.0.0.1:8085");

        enrichedTransactionAvroSerde.configure(serdeConfig, false);
        return enrichedTransactionAvroSerde;
    }
}
