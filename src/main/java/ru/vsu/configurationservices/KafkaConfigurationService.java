package ru.vsu.configurationservices;

import ru.vsu.clients.consumer.ConsumerService;

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

    }

    @Override
    public void close() throws Exception {

    }
}
