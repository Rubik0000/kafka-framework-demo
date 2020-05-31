package ru.vsu.clients.producer.api;

import org.apache.kafka.clients.producer.RecordMetadata;

@FunctionalInterface
public interface BatchCallback {

    void onCompletion(RecordMetadata metadata, Exception exception, int numberInBatch, int batchCount);
}
