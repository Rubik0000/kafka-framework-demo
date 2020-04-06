package ru.vsu.factories.producers.original;

import org.apache.kafka.clients.producer.Producer;

import java.util.Map;
import java.util.Properties;

public interface OriginalProducerFactory<K, V> {

    Producer<K, V> createProducer(Map<String, Object> configs);

    Producer<K, V> createProducer(Properties properties);

}
