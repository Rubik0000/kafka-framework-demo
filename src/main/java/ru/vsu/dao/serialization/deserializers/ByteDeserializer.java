package ru.vsu.dao.serialization.deserializers;

import ru.vsu.dao.serialization.exceptions.DeserializationException;

import java.util.Map;

public interface ByteDeserializer<T> {

    T deserialize(byte[] bytes) throws DeserializationException;
}
