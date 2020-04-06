package ru.vsu.configurationservices;

import java.util.Map;

public interface ConfigurationListener {

    void configure(Map<String, Object> configs);
}
