package ru.vsu.strategies.storage;

import org.apache.kafka.clients.producer.ProducerRecord;
import ru.vsu.dao.StoredProducerRecord;
import ru.vsu.dao.StoredProducerRecordsDao;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public final class PersistentStorageStrategy<K, V> implements StorageStrategy<K, V> {

    private final StoredProducerRecordsDao producerRecordsDao;
    private final StorageStrategy<K, V> strategy;


    public PersistentStorageStrategy(StoredProducerRecordsDao producerRecordsDao, StorageStrategy<K, V> strategy) {
        if (strategy instanceof PersistentStorageStrategy) {
            throw new IllegalArgumentException("Cannot use PersistentStorageStrategy as proxy strategy");
        }
        this.producerRecordsDao = producerRecordsDao;
        this.strategy = strategy;
    }


    @Override
    public void add(Collection<ProducerRecord<K, V>> producerRecords) {
        List<ProducerRecord<K, V>> storedRecords = producerRecords.stream()
                .map(record -> new StoredProducerRecord<>(UUID.randomUUID().toString(), false, record))
                .collect(Collectors.toList());
        for (ProducerRecord<K, V> record : storedRecords) {
            producerRecordsDao.add((StoredProducerRecord) record);
        }
        strategy.add(storedRecords);
    }

    @Override
    public Collection<ProducerRecord<K, V>> get() {
        return strategy.get();
    }

    @Override
    public Collection<ProducerRecord<K, V>> getAndRemove() {
        Collection<ProducerRecord<K, V>> records = strategy.getAndRemove();
        for (ProducerRecord<K, V> record : records) {
            producerRecordsDao.delete(((StoredProducerRecord) record).getId());
        }
        return records;
    }

    @Override
    public boolean isEmpty() {
        return strategy.isEmpty();
    }
}
