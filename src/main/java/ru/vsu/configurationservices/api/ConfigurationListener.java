package ru.vsu.configurationservices.api;

import java.util.Map;

public interface ConfigurationListener {

    void configure(Map<String, Object> configs);
}
