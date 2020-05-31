package ru.vsu.clients.consumer;

public interface ConsumerService<K, V> extends AutoCloseable {

    void subscribe(String topic, int levelOfPar, RecordListener<K, V> recordListener);

    void unsubscribe(String topic, RecordListener<K, V> recordListener);

    void unsubscribe(String topic);

}
