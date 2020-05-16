package ru.vsu.clients.producer.api;

import org.apache.kafka.clients.producer.RecordMetadata;

public interface BatchCallback {

    void onCompletion(RecordMetadata metadata, Exception exception, int numberInBatch, int batchCount);
}
