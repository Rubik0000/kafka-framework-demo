package ru.vsu.clients.consumer.impl;

import ru.vsu.clients.consumer.RecordListener;
import ru.vsu.configurationservices.api.ConfigurationListener;
import ru.vsu.factories.consumers.original.OriginalConsumerFactory;
import ru.vsu.utils.Utils;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractConsumerService<K, V> implements ConfigurationListener {

    private final OriginalConsumerFactory<K, V> consumerFactory;
    private Map<String, Object> consumerConfig;


    public AbstractConsumerService(OriginalConsumerFactory<K, V> consumerFactory, Map<String, Object> config) {
        if (consumerFactory == null) {
            throw new IllegalArgumentException("consumerFactory must not be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("Consumer config must not be null");
        }
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

    public final Map<String, Object> getConsumerConfig() {
        return consumerConfig;
    }

    public final OriginalConsumerFactory<K, V> getConsumerFactory() {
        return consumerFactory;
    }

    protected abstract void configure();

    protected void throwIfTopicIsNull(String topic) {
        if (topic == null) {
            throw new IllegalArgumentException("Topic must not be null");
        }
    }

    protected void throwIfRecordListenerIsNull(RecordListener<K, V> recordListener) {
        if (recordListener == null) {
            throw new IllegalArgumentException("Record listener must not be null");
        }
    }

    protected void throwIfLevelOfParallelismIfLessThanOne(int numberOfPar) {
        if (numberOfPar <= 0) {
            throw new IllegalArgumentException("The level of parallelism cannot be less then 1");
        }
    }
}
