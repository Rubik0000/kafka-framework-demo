package ru.vsu.dao;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.SQLException;
import java.util.Collection;

public interface StoredProducerRecordsDao {

    void add(StoredProducerRecord record) throws SQLException;

    void update(StoredProducerRecord record);

    void delete(String id) throws SQLException;

    StoredProducerRecord getById(String id) throws SQLException;

    Collection<StoredProducerRecord> getUnsentRecords() throws SQLException;
}
