package com.ilham.github.producer;

import com.ilham.github.avro.Author;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

import static com.ilham.github.config.KafkaStreamsConfig.getProducerProperties;

public class AuthorProducer {

    private static Logger logger = LoggerFactory.getLogger(AuthorProducer.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String topic = "author-example-topic";

        Author author = Author.newBuilder()
                .setId(1)
                .setName("J.K. Rowling")
                .build();

        KafkaProducer<Integer, Author> producer = new KafkaProducer<>(getProducerProperties());
        ProducerRecord<Integer,Author> producerRecord = new ProducerRecord<>(topic,author.getId(),author);
        producer.send(producerRecord).get();

        logger.info("Sent message: " + author);

        producer.flush();
        producer.close();
    }
}
