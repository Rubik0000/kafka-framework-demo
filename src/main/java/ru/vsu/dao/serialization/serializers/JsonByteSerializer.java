package ru.vsu.dao.serialization.serializers;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import ru.vsu.dao.StoredProducerRecord;
import ru.vsu.dao.serialization.exceptions.DeserializationException;
import ru.vsu.dao.serialization.exceptions.SerializationException;

public class JsonByteSerializer implements ByteSerializer<StoredProducerRecord> {

    //private final TypeReference<T> typeReference = new TypeReference<T>(){};
    private final ObjectMapper objectMapper = new ObjectMapper();


    @Override
    public byte[] serialize(Object o) throws SerializationException {
        try {
            return objectMapper.writeValueAsBytes(o);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public StoredProducerRecord deserialize(byte[] bytes) throws DeserializationException {
        try {
            return objectMapper.readValue(bytes, StoredProducerRecord.class);
        } catch (Exception e) {
            throw new DeserializationException(e);
        }
    }
}
