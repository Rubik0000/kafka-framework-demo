package ru.vsu.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import ru.vsu.dao.pojo.ProducerRecordPojo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;

public class ApacheDerbyDao implements StoredProducerRecordsDao, AutoCloseable {

    private static final String TABLE_NAME = "records";

    private static final String ID = "id";
    private static final String WAS_SENT = "was_sent";
    private static final String RECORD = "record";

    private static final String CREATE_TABLE_SQL = MessageFormat.format(
            "CREATE TABLE {0}(" +
            "{1} VARCHAR(50)," +
            "{2} BOOLEAN," +
            "{3} VARCHAR(32672)," +
            "PRIMARY KEY ({1})" +
            ")", TABLE_NAME, ID, WAS_SENT, RECORD);

    private static final String INSERT_SQL = MessageFormat.format(
            "INSERT INTO {0} ({1}, {2}, {3})" +
            "VALUES (?, ?, ?)", TABLE_NAME, ID, WAS_SENT, RECORD);

    private static final String SELECT_UNSET_RECORDS_SQL = MessageFormat.format(
            "SELECT {1}, {2}, {3} FROM {0} WHERE {2} = false", TABLE_NAME, ID, WAS_SENT, RECORD);

    private static final String SELECT_BY_ID_RECORDS_SQL = MessageFormat.format(
            "SELECT {1}, {2}, {3} FROM {0} WHERE {1} = ?", TABLE_NAME, ID, WAS_SENT, RECORD);

    private static final String DELETE_BY_ID_SQL = MessageFormat.format(
            "DELETE FROM {0} WHERE {1} = ?", TABLE_NAME, ID);


    private Connection connection;
    private ObjectMapper objectMapper = new ObjectMapper();


    public ApacheDerbyDao() throws SQLException {
        //Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        connection = DriverManager.getConnection("jdbc:derby:testdb;create=true");
        if (!isTableExist()) {
            System.out.println(String.format("Create '%s' table", TABLE_NAME));
            try (Statement statement = connection.createStatement()) {
                statement.execute(CREATE_TABLE_SQL);
            }
        }
    }

    @Override
    public void add(StoredProducerRecord record) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(INSERT_SQL)) {
            preparedStatement.setString(1, record.getId());
            preparedStatement.setBoolean(2, record.wasSent());
            preparedStatement.setString(3, objectMapper.writeValueAsString(record.getProducerRecordPojo()));
            preparedStatement.execute();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void update(StoredProducerRecord record) {

    }

    @Override
    public void delete(String id) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(DELETE_BY_ID_SQL)) {
            preparedStatement.setString(1, id);
            preparedStatement.execute();
        }
    }

    @Override
    public StoredProducerRecord getById(String id) throws SQLException {
        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_BY_ID_RECORDS_SQL)) {
            preparedStatement.setString(1, id);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (!resultSet.next()) {
                    return null;
                }
                try {
                    return getRecordFromDb(resultSet);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }
    }

    @Override
    public Collection<StoredProducerRecord> getUnsentRecords() throws SQLException {
        Collection<StoredProducerRecord> unsentRecords = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet unsentRecordsRs = statement.executeQuery(SELECT_UNSET_RECORDS_SQL)) {
            while (unsentRecordsRs.next()) {
                unsentRecords.add(getRecordFromDb(unsentRecordsRs));
            }
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return unsentRecords;
    }

    @Override
    public void close() throws Exception {
        connection.close();
        try {
            DriverManager.getConnection("jdbc:derby:;shutdown=true");
        } catch (Exception e) { }
    }

    private boolean isTableExist() throws SQLException {
        try (ResultSet tables = connection.getMetaData().getTables(null, null, null, new String[]{"TABLE"})) {
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                if (TABLE_NAME.equalsIgnoreCase(tableName)) {
                    return true;
                }
            }
            return false;
        }
    }

    private byte[] objectToBytes(Object object) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(object);
            oos.flush();
            return bos.toByteArray();
        }
    }

    private StoredProducerRecord getRecordFromDb(ResultSet resultSet) throws SQLException, JsonProcessingException {
        String id = resultSet.getString(ID);
        String jsonRecord = resultSet.getString(RECORD);
        ProducerRecordPojo producerRecord = objectMapper.readValue(jsonRecord, ProducerRecordPojo.class);
        return new StoredProducerRecord(id, resultSet.getBoolean(WAS_SENT), producerRecord);
    }
}
