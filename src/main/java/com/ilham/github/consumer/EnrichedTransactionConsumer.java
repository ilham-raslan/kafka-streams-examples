package com.ilham.github.consumer;

import com.ilham.github.avro.EnrichedTransaction;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;

import static com.ilham.github.config.KafkaStreamsConfig.getConsumerProperties;

public class EnrichedTransactionConsumer {

    private static Logger logger = LoggerFactory.getLogger(EnrichedTransactionConsumer.class);

    public static void main(String[] args) {
        String topic = "enriched-transaction-example-topic";
        KafkaConsumer<Integer, EnrichedTransaction> consumer = new KafkaConsumer<>(getConsumerProperties());
        consumer.subscribe(Collections.singleton(topic));

        try {
            while (true) {
                final ConsumerRecords<Integer, EnrichedTransaction> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                consumerRecords.forEach(record -> logger.info("Received message: " + record.value()));
                consumer.commitAsync();
            }
        }
        catch (WakeupException ignored) {

        }
        finally {
            consumer.close();
            logger.info("Consumer is closing now");
        }
    }
}
