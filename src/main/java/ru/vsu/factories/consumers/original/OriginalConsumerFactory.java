package ru.vsu.factories.consumers.original;

import org.apache.kafka.clients.consumer.Consumer;

import java.util.Map;

public interface OriginalConsumerFactory<K, V> {

    Consumer<K, V> createConsumer(Map<String, Object> configs);
}
