package ru.vsu.clients.consumer;

import java.util.Collection;
import java.util.Collections;

public interface SubscribeConsumerService<K, V> extends AutoCloseable {

    void subscribe(String topic, int numberOfPar, RecordListener<K, V> recordListener);

}
