package com.ilham.github.joiner;

import com.ilham.github.avro.Author;
import com.ilham.github.avro.Book;
import com.ilham.github.avro.EnrichedBook;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class EnrichedBookJoiner implements ValueJoiner<Book, Author, EnrichedBook> {
    @Override
    public EnrichedBook apply(Book book, Author author) {
        return EnrichedBook.newBuilder()
                .setId(book.getId())
                .setTitle(book.getTitle())
                .setAuthorName(author.getName())
                .build();
    }
}
