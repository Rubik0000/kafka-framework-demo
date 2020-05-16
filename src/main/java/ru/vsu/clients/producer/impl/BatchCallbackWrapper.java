package ru.vsu.clients.producer.impl;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import ru.vsu.clients.producer.api.BatchCallback;

public class BatchCallbackWrapper implements Callback {

    private int numberInBatch;
    private int batchCount;
    private BatchCallback batchCallback;


    public BatchCallbackWrapper(int numberInBatch, int batchCount, BatchCallback batchCallback) {
        this.numberInBatch = numberInBatch;
        this.batchCount = batchCount;
        this.batchCallback = batchCallback;
    }


    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        batchCallback.onCompletion(metadata, exception, numberInBatch, batchCount);
    }
}
