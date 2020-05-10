package ru.vsu.strategies.storage;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collection;

public interface QueueStorageStrategy<K, V> {

    void add(Collection<ProducerRecord<K, V>> records);

    Collection<ProducerRecord<K, V>> get();

    Collection<ProducerRecord<K, V>> getAndRemove();

    boolean isEmpty();

}
