package ru.vsu.factories.consumers.original;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Map;

public class OriginalKafkaConsumerFactory<K, V> implements OriginalConsumerFactory<K, V> {

    @Override
    public Consumer<K, V> createConsumer(Map<String, Object> configs) {
        return new KafkaConsumer<>(configs);
    }
}
