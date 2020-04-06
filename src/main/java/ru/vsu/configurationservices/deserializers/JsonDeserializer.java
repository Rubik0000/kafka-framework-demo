package ru.vsu.configurationservices.deserializers;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class JsonDeserializer implements ConfigurationDeserializer {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Map<String, Object> deserialize(byte[] bytes) throws Exception {
        return objectMapper.readValue(bytes, Map.class);
    }
}
