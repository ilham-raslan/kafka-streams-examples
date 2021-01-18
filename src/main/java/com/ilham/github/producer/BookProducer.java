package com.ilham.github.producer;

import com.ilham.github.avro.Book;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

import static com.ilham.github.config.KafkaStreamsConfig.getProducerProperties;

public class BookProducer {

    private static Logger logger = LoggerFactory.getLogger(BookProducer.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String topic = "book-example-topic";

        Book book = Book.newBuilder()
                .setId(3)
                .setTitle("Harry Potter and the Chamber of Secrets")
                .setAuthorId(1)
                .build();

        KafkaProducer<Integer, Book> producer = new KafkaProducer<>(getProducerProperties());
        ProducerRecord<Integer,Book> producerRecord = new ProducerRecord<>(topic,book.getId(),book);
        producer.send(producerRecord).get();

        logger.info("Sent message: " + book);

        producer.flush();
        producer.close();
    }
}
