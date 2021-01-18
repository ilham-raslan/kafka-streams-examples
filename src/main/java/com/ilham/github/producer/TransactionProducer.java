package com.ilham.github.producer;

import com.ilham.github.avro.Transaction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

import static com.ilham.github.config.KafkaStreamsConfig.getProducerProperties;

public class TransactionProducer {

    private static Logger logger = LoggerFactory.getLogger(TransactionProducer.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String topic = "transaction-example-topic";

        Transaction transaction = Transaction.newBuilder()
                .setId(1)
                .setBookId(3)
                .build();

        KafkaProducer<Integer, Transaction> producer = new KafkaProducer<>(getProducerProperties());
        ProducerRecord<Integer,Transaction> producerRecord = new ProducerRecord<>(topic,transaction.getId(),transaction);
        producer.send(producerRecord).get();

        logger.info("Sent message: " + transaction);

        producer.flush();
        producer.close();
    }
}
