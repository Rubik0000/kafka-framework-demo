package ru.vsu.strategies.send;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public interface SendStrategyCallback<K, V> {

    void onCompletion(ProducerRecord<K, V> producerRecord, RecordMetadata metadata, Exception exception);
}
