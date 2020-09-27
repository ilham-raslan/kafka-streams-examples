package com.ilham.github.producer;

import com.ilham.github.avro.Customer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

import static com.ilham.github.config.KafkaStreamsConfig.getProducerProperties;

public class CustomerProducer {

    private static Logger logger = LoggerFactory.getLogger(CustomerProducer.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String topic = "customer-example-topic";

        Customer customer = Customer.newBuilder()
                .setId(1)
                .setName("Ilham Raslan")
                .build();

        KafkaProducer<Integer, Customer> producer = new KafkaProducer<>(getProducerProperties());
        ProducerRecord<Integer,Customer> producerRecord = new ProducerRecord<>(topic,customer.getId(),customer);
        producer.send(producerRecord).get();

        logger.info("Sent message: " + customer);

        producer.flush();
        producer.close();
    }
}
