package ru.vsu.utils;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class Utils {

    public static Map<String, Object> propertiesToMap(Properties properties) {
        return properties.entrySet().stream().collect(Collectors.toMap(k -> k.getKey().toString(), Map.Entry::getValue));
    }

    public static Map<String, Object> convertMap(Map<String, String> map) {
        return map.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
