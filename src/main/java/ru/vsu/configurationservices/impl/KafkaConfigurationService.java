package ru.vsu.configurationservices.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import ru.vsu.clients.consumer.ConsumerService;
import ru.vsu.configurationservices.api.ConfigurationListener;
import ru.vsu.configurationservices.api.ConfigurationService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class KafkaConfigurationService implements ConfigurationService {

    private final ConsumerService<String, String> consumerService;
    private final ObjectMapper objectMapper;
    private final Map<String, List<ConfigurationListener>> listeners;
    private final Map<String, Map<String, Object>> actualConfigs;


    public KafkaConfigurationService(
            ConsumerService<String, String> consumerService,
            String configurationTopic) {
        this.consumerService = consumerService;
        this.objectMapper = new ObjectMapper();
        this.listeners = new ConcurrentHashMap<>();
        this.actualConfigs = new ConcurrentHashMap<>();
        consumerService.subscribe(configurationTopic, 1, this::listen);
    }


    @Override
    public Map<String, Object> getConfiguration(String configName) throws Exception {
        return actualConfigs.get(configName);
    }

    @Override
    public void registerListener(String configName, ConfigurationListener configurationListener) {
        listeners.putIfAbsent(configName, new CopyOnWriteArrayList<>());
        listeners.get(configName).add(configurationListener);
    }

    @Override
    public void unregisterListener(String configName, ConfigurationListener configurationListener) {
        List<ConfigurationListener> configurationListeners = listeners.get(configName);
        if (configurationListeners != null) {
            int i = configurationListeners.indexOf(configurationListener);
            if (i >= 0) {
                configurationListeners.remove(i);
            }
        }
    }

    @Override
    public void unregisterListener(String configName) {
        listeners.remove(configName);
    }

    @Override
    public void close() throws Exception {
        consumerService.close();
    }

    private void listen(ConsumerRecord<String, String> record) {
        try {
            Map<String, Object> config = objectMapper.readValue(record.value(), Map.class);
            actualConfigs.put(record.key(), config);
            if (listeners.containsKey(record.key()) && listeners.get(record.key()) != null) {

                listeners.get(record.key()).forEach(l -> l.configure(config));
                System.out.println("Get new config " + config);
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
