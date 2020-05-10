package ru.vsu.dao.serialization.deserializers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import ru.vsu.dao.serialization.exceptions.DeserializationException;

import java.io.IOException;
import java.util.Map;

public class JsonByteDeserializer<T> implements ByteDeserializer<T> {

    private final TypeReference<T> typeReference = new TypeReference<T>(){};
    private final ObjectMapper objectMapper = new ObjectMapper();


    @Override
    public T deserialize(byte[] bytes) throws DeserializationException {
        try {
            return objectMapper.readValue(bytes, typeReference);
        } catch (Exception e) {
            throw new DeserializationException(e);
        }
    }
}
