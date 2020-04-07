package ru.vsu.dao.pojo;

import org.apache.kafka.common.header.Header;

public class HeaderPojo {

    private String key;
    private byte[] value;


    public HeaderPojo() { }

    public HeaderPojo(Header header) {
        key = header.key();
        value = header.value();
    }


    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }
}
