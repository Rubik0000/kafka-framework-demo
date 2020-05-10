package ru.vsu.dao;

import ru.vsu.dao.pojo.ProducerRecordPojo;

import java.io.Serializable;

public class StoredProducerRecord implements Serializable {

    private String id;
    private boolean wasSent;
    private ProducerRecordPojo producerRecordPojo;


    public StoredProducerRecord() { }

    public StoredProducerRecord(String id, boolean wasSent, ProducerRecordPojo producerRecordPojo) {
        this.id = id;
        this.wasSent = wasSent;
        this.producerRecordPojo = producerRecordPojo;
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public boolean wasSent() {
        return wasSent;
    }

    public void setWasSent(boolean wasSent) {
        this.wasSent = wasSent;
    }


    public ProducerRecordPojo getProducerRecordPojo() {
        return producerRecordPojo;
    }

    public void setProducerRecordPojo(ProducerRecordPojo producerRecordPojo) {
        this.producerRecordPojo = producerRecordPojo;
    }
}
