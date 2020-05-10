package ru.vsu.dao.serialization.serializers;

import ru.vsu.dao.serialization.exceptions.DeserializationException;
import ru.vsu.dao.serialization.exceptions.SerializationException;

public interface ByteSerializer<T> {

    byte[] serialize(Object o) throws SerializationException;

    T deserialize(byte[] bytes) throws DeserializationException;
}
