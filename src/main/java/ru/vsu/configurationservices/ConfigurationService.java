package ru.vsu.configurationservices;

import java.util.Map;

public interface ConfigurationService extends AutoCloseable {

    Map<String, Object> getConfiguration(String configName) throws Exception;

    void registerListener(String configName, ConfigurationListener configurationListener);
}
