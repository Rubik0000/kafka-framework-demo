package ru.vsu.clients.producer.impl;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerRecordWithCallback<K, V> extends ProducerRecord<K, V> {

    private final Callback callback;


    public ProducerRecordWithCallback(ProducerRecord<K, V> record, Callback callback) {
        super(record.topic(), record.partition(), record.timestamp(), record.key(), record.value(), record.headers());
        this.callback = callback;
    }


    public Callback callback() {
        return callback;
    }
}
