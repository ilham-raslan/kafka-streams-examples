package com.ilham.github.producer;

import com.ilham.github.avro.Book;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static com.ilham.github.config.KafkaStreamsConfig.getProducerProperties;

public class BookProducer {
    public static void main(String[] args) {
        String topic = "book-example-topic";

        Book book = Book.newBuilder()
                .setId(1)
                .setTitle("Harry Potter and the Goblet of Fire")
                .setAuthorId(1)
                .build();

        KafkaProducer<Integer, Book> producer = new KafkaProducer<>(getProducerProperties());
        ProducerRecord<Integer,Book> producerRecord = new ProducerRecord<>(topic,book);
        producer.send(producerRecord);

        System.out.println("Message sent");

        producer.flush();
        producer.close();
    }
}
