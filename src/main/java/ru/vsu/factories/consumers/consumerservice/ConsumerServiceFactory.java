package ru.vsu.factories.consumers.consumerservice;

import ru.vsu.clients.consumer.ConsumerService;
import ru.vsu.clients.consumer.PartitionConsumerService;

import java.util.Map;
import java.util.Properties;

public interface ConsumerServiceFactory {

    <K, V> ConsumerService<K, V> createConsumerService(Map<String, Object> config);

    <K, V> ConsumerService<K, V> createConsumerService(Properties config);

    <K, V> PartitionConsumerService<K, V> createPartitionConsumerService(Map<String, Object> config);

    <K, V> PartitionConsumerService<K, V> createPartitionConsumerService(Properties config);
}
