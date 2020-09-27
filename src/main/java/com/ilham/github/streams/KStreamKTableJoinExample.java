package com.ilham.github.streams;

import com.ilham.github.avro.Book;
import com.ilham.github.avro.EnrichedTransaction;
import com.ilham.github.avro.Transaction;
import com.ilham.github.joiner.EnrichedTransactionJoiner;
import com.ilham.github.producer.TransactionProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ilham.github.config.KafkaStreamsConfig.*;

public class KStreamKTableJoinExample {

    private static Logger logger = LoggerFactory.getLogger(KStreamKTableJoinExample.class);

    /*
        This is an example of a KStream-KTable join. In this, a transaction is put through that only contains a unique
        transaction id and the book id of the book transacted. This is enriched with the book table to obtain the name
        of the book for each transaction that comes in.
     */

    public static void main(String[] args) {
        Topology topology = buildTopology();

        KafkaStreams streams = new KafkaStreams(topology, getKafkaStreamsProperties());

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
            }
        });

        streams.start();
    }

    public static Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        KTable<Integer, Book> bookTable = builder.table("book-example-topic", Materialized.<Integer, Book, KeyValueStore<Bytes, byte[]>>as("book-table").withKeySerde(Serdes.Integer()).withValueSerde(getBookAvroSerde()));

        KStream<Integer, Transaction> transactionStream = builder.stream("transaction-example-topic", Consumed.with(Serdes.Integer(),getTransactionAvroSerde()));

        KStream<Integer,EnrichedTransaction> enrichedTransactionStream = transactionStream
                .selectKey((key,value)->value.getBookId())
                .join(bookTable, new EnrichedTransactionJoiner(),Joined.<Integer,Transaction,Book>with(Serdes.Integer(),getTransactionAvroSerde(),getBookAvroSerde()))
                .selectKey((key,value)->value.getId());

        enrichedTransactionStream
                .peek((key,value)-> logger.info("Sending " + value + " to topic"))
                .to("enriched-transaction-example-topic",Produced.with(Serdes.Integer(),getEnrichedTransactionAvroSerde()));

        return builder.build();
    }
}
