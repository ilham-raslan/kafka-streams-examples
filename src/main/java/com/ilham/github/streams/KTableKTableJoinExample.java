package com.ilham.github.streams;

import com.ilham.github.avro.Author;
import com.ilham.github.avro.Book;
import com.ilham.github.avro.EnrichedBook;
import com.ilham.github.joiner.EnrichedBookJoiner;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ilham.github.config.KafkaStreamsConfig.*;

public class KTableKTableJoinExample {

    private static Logger logger = LoggerFactory.getLogger(KTableKTableJoinExample.class);

    /*
        This is an example of a KTable-KTable join using a foreign key. In this, the book table is joined with the author
        table to obtain the name of the author corresponding to the author id of the book record in the book table. This
        results in the final entity, the enriched book record that contains the name of both the book and author.
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

        KTable<Integer, Author> authorTable = builder.table("author-example-topic", Materialized.<Integer, Author, KeyValueStore<Bytes,byte[]>>as("author-table").withKeySerde(Serdes.Integer()).withValueSerde(getAuthorAvroSerde()));

        KTable<Integer, EnrichedBook> enrichedBookTable = bookTable.join(authorTable,Book::getAuthorId, new EnrichedBookJoiner(),Materialized.<Integer,EnrichedBook,KeyValueStore<Bytes,byte[]>>as("enriched-book-table").withKeySerde(Serdes.Integer()).withValueSerde(getEnrichedBookAvroSerde()));

        enrichedBookTable.toStream()
                .peek((key,value)-> logger.info("Sending " + value + " to topic"))
                .to("enriched-book-example-topic", Produced.with(Serdes.Integer(),getEnrichedBookAvroSerde()));

        return builder.build();
    }
}
