package ru.vsu.dao;

import org.apache.kafka.clients.producer.ProducerRecord;

public class StoredProducerRecord<K, V> extends ProducerRecord<K, V> {

    private String id;
    private boolean wasSent;


    public StoredProducerRecord(String id, boolean wasSent, ProducerRecord<K, V> producerRecord) {
        super(
                producerRecord.topic(),
                producerRecord.partition(),
                producerRecord.timestamp(),
                producerRecord.key(),
                producerRecord.value(),
                producerRecord.headers());
        this.id = id;
        this.wasSent = wasSent;
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public boolean isWasSent() {
        return wasSent;
    }

    public void setWasSent(boolean wasSent) {
        this.wasSent = wasSent;
    }
}
