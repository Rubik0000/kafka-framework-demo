package ru.vsu.configurationservices.deserializers;

import java.util.Map;

public interface ConfigurationDeserializer {

    Map<String, Object> deserialize(byte[] bytes) throws Exception;
}
