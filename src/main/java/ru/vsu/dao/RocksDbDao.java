package ru.vsu.dao;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import ru.vsu.dao.serialization.serializers.ByteSerializer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

public class RocksDbDao implements StoredProducerRecordsDao, AutoCloseable {

    private final Options options;
    private final RocksDB dataBase;
    private final ByteSerializer<StoredProducerRecord> serializer;

    public RocksDbDao(ByteSerializer<StoredProducerRecord> serializer) throws RocksDBException {
        this.serializer = serializer;
        RocksDB.loadLibrary();
        options = new Options();
        options.setCreateIfMissing(true);
        dataBase = RocksDB.open(options, "rocksdb");
    }


    @Override
    public void add(StoredProducerRecord record) throws SQLException {
        try {
            dataBase.put(record.getId().getBytes(), serializer.serialize(record));
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void update(StoredProducerRecord record) {

    }

    @Override
    public void delete(String id) throws SQLException {
        try {
            dataBase.delete(id.getBytes());
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public StoredProducerRecord getById(String id) throws SQLException {
        try {
            if (id == null) {
                return null;
            }
            byte[] bytes = dataBase.get(id.getBytes());
            if (bytes != null) {
                return serializer.deserialize(bytes);
            }
            return null;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public Collection<StoredProducerRecord> getUnsentRecords() throws SQLException {
        try {
            RocksIterator rocksIterator = dataBase.newIterator();
            ArrayList<StoredProducerRecord> unsentRecords = new ArrayList<>();
            for (rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()) {
                StoredProducerRecord record = serializer.deserialize(rocksIterator.value());
                if (!record.wasSent()) {
                    unsentRecords.add(record);
                }
            }
            return unsentRecords;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void close() throws Exception {
        dataBase.close();
        options.close();
    }
}
