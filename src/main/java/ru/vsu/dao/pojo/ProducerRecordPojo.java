package ru.vsu.dao.pojo;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

import java.util.ArrayList;
import java.util.List;

public class ProducerRecordPojo {

    private String topic;
    private Integer partition;
    private List<HeaderPojo> headers;
    private Object key;
    private Object value;
    private Long timestamp;


    public ProducerRecordPojo() { }

    public ProducerRecordPojo(ProducerRecord record) {
        topic = record.topic();
        partition = record.partition();
        key = record.key();
        value = record.value();
        timestamp = record.timestamp();
        headers = new ArrayList<>();
        if (record.headers() != null) {
            record.headers().forEach(header -> headers.add(new HeaderPojo(header)));
        }
    }


    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public List<HeaderPojo> getHeaders() {
        return headers;
    }

    public void setHeaders(List<HeaderPojo> headers) {
        this.headers = headers;
    }

    public Object getKey() {
        return key;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
