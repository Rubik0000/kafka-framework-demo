package ru.vsu.configurationservices.api;

import java.util.Map;

public interface ConfigurationService extends AutoCloseable {

    Map<String, Object> getConfiguration(String configName) throws Exception;

    void registerListener(String configName, ConfigurationListener configurationListener);

    void unregisterListener(String configName, ConfigurationListener configurationListener);

    void unregisterListener(String configName);
}
