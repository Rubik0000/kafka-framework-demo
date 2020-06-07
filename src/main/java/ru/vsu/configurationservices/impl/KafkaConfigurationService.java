package ru.vsu.configurationservices.impl;

import ru.vsu.clients.consumer.ConsumerService;
import ru.vsu.configurationservices.api.ConfigurationListener;
import ru.vsu.configurationservices.api.ConfigurationService;

import java.util.Map;

public class KafkaConfigurationService implements ConfigurationService {

    private ConsumerService<String, String> consumerService;


    public KafkaConfigurationService(ConsumerService<String, String> consumerService) {
        this.consumerService = consumerService;
    }


    @Override
    public Map<String, Object> getConfiguration(String configName) throws Exception {
        return null;
    }

    @Override
    public void registerListener(String configName, ConfigurationListener configurationListener) {
        consumerService.subscribe(configName, 1, null);
    }

    @Override
    public void close() throws Exception {

    }
}
