package ru.vsu.factories.producers.original;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import ru.vsu.utils.Utils;

import java.util.Map;
import java.util.Properties;

public class OriginalKafkaProducerFactory<K, V> implements OriginalProducerFactory<K, V> {

    @Override
    public Producer<K, V> createProducer(Map<String, Object> configs) {
        return new KafkaProducer<>(configs);
    }

    @Override
    public Producer<K, V> createProducer(Properties properties) {
        return new KafkaProducer<>(properties);
    }
}
