package com.ilham.github.joiner;

import com.ilham.github.avro.Book;
import com.ilham.github.avro.EnrichedTransaction;
import com.ilham.github.avro.Transaction;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class EnrichedTransactionJoiner implements ValueJoiner<Transaction, Book, EnrichedTransaction> {
    @Override
    public EnrichedTransaction apply(Transaction transaction, Book book) {
        return EnrichedTransaction.newBuilder()
                .setId(transaction.getId())
                .setBookName(book.getTitle())
                .build();
    }
}
