package ru.vsu.factories.consumers.consumerservice;

import ru.vsu.clients.consumer.ConsumerService;
import ru.vsu.clients.consumer.PartitionConsumerService;
import ru.vsu.clients.consumer.impl.GroupManagedKafkaConsumerService;
import ru.vsu.clients.consumer.impl.PartitionManagedKafkaConsumerService;
import ru.vsu.factories.consumers.original.OriginalKafkaConsumerFactory;
import ru.vsu.utils.Utils;

import java.util.Map;
import java.util.Properties;

public class KafkaConsumerServiceFactory implements ConsumerServiceFactory {


    @Override
    public <K, V> ConsumerService<K, V> createConsumerService(Map<String, Object> config) {
        return new GroupManagedKafkaConsumerService<>(new OriginalKafkaConsumerFactory<>(), config);
    }

    @Override
    public <K, V> ConsumerService<K, V> createConsumerService(Properties config) {
        return createConsumerService(Utils.propertiesToMap(config));
    }

    @Override
    public <K, V> PartitionConsumerService<K, V> createPartitionConsumerService(Map<String, Object> config) {
        return new PartitionManagedKafkaConsumerService<K, V>(new OriginalKafkaConsumerFactory<>(), config);
    }

    @Override
    public <K, V> PartitionConsumerService<K, V> createPartitionConsumerService(Properties config) {
        return createPartitionConsumerService(Utils.propertiesToMap(config));
    }
}
