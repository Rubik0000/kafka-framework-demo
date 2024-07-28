package ru.vsu.strategies.storage;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import ru.vsu.dao.StoredProducerRecord;
import ru.vsu.dao.StoredProducerRecordsDao;
import ru.vsu.dao.pojo.ProducerRecordPojo;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

public class PersistentQueueStorageStrategy<K, V> implements QueueStorageStrategy<K, V> {

    private final StoredProducerRecordsDao producerRecordsDao;
    private final Queue<String> queue;


    public PersistentQueueStorageStrategy(StoredProducerRecordsDao producerRecordsDao) {
        this.producerRecordsDao = producerRecordsDao;
        this.queue = new LinkedBlockingDeque<>();
        try {
            List<StoredProducerRecord> unsentRecords = new ArrayList<>(producerRecordsDao.getUnsentRecords());
            unsentRecords.sort((storedProducerRecord, t1) -> (int) (storedProducerRecord.getProducerRecordPojo().getDbTimestamp() - t1.getProducerRecordPojo().getDbTimestamp()));
            unsentRecords.forEach(record -> queue.add(record.getId()));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void add(Collection<ProducerRecord<K, V>> producerRecords) {
        for (ProducerRecord<K, V> record : producerRecords) {
            try {
                String id = UUID.randomUUID().toString();
                producerRecordsDao.add(new StoredProducerRecord(id, false, new ProducerRecordPojo(record, System.currentTimeMillis())));
                queue.add(id);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public Collection<ProducerRecord<K, V>> get() {
        try {
            return wrapIntoCollection(producerRecordsDao.getById(queue.peek()));
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Collection<ProducerRecord<K, V>> getAndRemove() {
        try {
            String id = queue.poll();
            Collection<ProducerRecord<K, V>> producerRecords = wrapIntoCollection(producerRecordsDao.getById(id));
            producerRecordsDao.delete(id);
            return producerRecords;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    private Collection<ProducerRecord<K, V>> wrapIntoCollection(StoredProducerRecord record) {
        if (record == null) {
            return Collections.emptyList();
        }
        ProducerRecordPojo producerRecordPojo = record.getProducerRecordPojo();
        Headers headers = new RecordHeaders();
        if (producerRecordPojo.getHeaders() != null) {
            producerRecordPojo.getHeaders().forEach(h -> headers.add(new RecordHeader(h.getKey(), h.getValue())));
        }
        ProducerRecord<K, V> producerRecord = new ProducerRecord<>(
                producerRecordPojo.getTopic(),
                producerRecordPojo.getPartition(),
                producerRecordPojo.getTimestamp(),
                (K) producerRecordPojo.getKey(),
                (V) producerRecordPojo.getValue(),
                headers
        );
        return Collections.singletonList(producerRecord);
    }
}
