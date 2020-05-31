package ru.vsu.clients.consumer.impl;

import ru.vsu.configurationservices.api.ConfigurationListener;
import ru.vsu.factories.consumers.original.OriginalConsumerFactory;
import ru.vsu.utils.Utils;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractConsumerService<K, V> implements ConfigurationListener {

    private OriginalConsumerFactory<K, V> consumerFactory;
    private Map<String, Object> consumerConfig;


    public AbstractConsumerService(OriginalConsumerFactory<K, V> consumerFactory, Map<String, Object> config) {
        this.consumerFactory = consumerFactory;
        this.consumerConfig = new ConcurrentHashMap<>(config);
    }

    public AbstractConsumerService(OriginalConsumerFactory<K, V> consumerFactory, Properties properties) {
        this(consumerFactory, Utils.propertiesToMap(properties));
    }

    @Override
    public final void configure(Map<String, Object> configs) {
        consumerConfig = configs;
        configure();
    }

    protected abstract void configure();

    public final Map<String, Object> getConsumerConfig() {
        return consumerConfig;
    }

    public final OriginalConsumerFactory<K, V> getConsumerFactory() {
        return consumerFactory;
    }
}
