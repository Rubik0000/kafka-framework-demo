package ru.vsu.strategies.storage;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

public class StoreByOneStrategy<K, V> implements StorageStrategy<K, V> {

    private final Queue<ProducerRecord<K, V>> queue = new LinkedBlockingDeque<>();


    @Override
    public void add(Collection<ProducerRecord<K, V>> records) {
        queue.addAll(records);
    }

    @Override
    public Collection<ProducerRecord<K, V>> get() {
        return wrapIntoCollection(queue.peek());
    }

    @Override
    public Collection<ProducerRecord<K, V>> getAndRemove() {
        return wrapIntoCollection(queue.poll());
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    private Collection<ProducerRecord<K, V>> wrapIntoCollection(ProducerRecord<K, V> record) {
        if (record == null) {
            return Collections.emptyList();
        }
        return Collections.singletonList(record);
    }
}
