package ru.vsu.clients.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface RecordListener<K, V> {

    void listen(ConsumerRecords<K, V> records);
}
