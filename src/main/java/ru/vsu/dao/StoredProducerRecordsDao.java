package ru.vsu.dao;

public interface StoredProducerRecordsDao {

    void add(StoredProducerRecord record);

    void update(StoredProducerRecord record);

    void delete(String id);

    StoredProducerRecord getById(String id);
}
