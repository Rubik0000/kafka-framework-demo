package ru.vsu.configurationservices.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import ru.vsu.configurationservices.api.ConfigurationListener;
import ru.vsu.configurationservices.api.ConfigurationService;
import ru.vsu.dao.serialization.deserializers.ByteDeserializer;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ZookeeperConfigurationService implements ConfigurationService {

    public static final String CONFIG_STORAGE_PATH = "/configservice/";


    private final CuratorFramework curatorFramework;
    private final ByteDeserializer<Map<String, Object>> deserializer;
    private final Map<String, Set<ConfigurationListener>> listeners;


    public ZookeeperConfigurationService(CuratorFramework curatorFramework, ByteDeserializer<Map<String, Object>> deserializer) {
        this.curatorFramework = curatorFramework;
        this.deserializer = deserializer;
        this.listeners = new ConcurrentHashMap<>();
    }


    @Override
    public Map<String, Object> getConfiguration(String configName) throws Exception {
        byte[] data = curatorFramework.getData().forPath(CONFIG_STORAGE_PATH + configName);
        return deserializer.deserialize(data);
    }

    @Override
    public void registerListener(String configName, ConfigurationListener configurationListener) {
        String path = CONFIG_STORAGE_PATH + configName;
        listeners.computeIfAbsent(path, key -> {
            try {
                curatorFramework.getData().usingWatcher(new ConfigWatcher(path)).forPath(path);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return new HashSet<>();
        });
        listeners.get(path).add(configurationListener);
    }

    @Override
    public void close() throws Exception {
        curatorFramework.close();
    }


    class ConfigWatcher implements CuratorWatcher {

        private final String configName;


        public ConfigWatcher(String configName) {
            this.configName = configName;
        }


        @Override
        public void process(WatchedEvent event) throws Exception {
            if (event.getType() == Watcher.Event.EventType.NodeDataChanged || event.getType() == Watcher.Event.EventType.NodeCreated) {
                byte[] rawConfig = curatorFramework.getData().forPath(event.getPath());
                Map<String, Object> config = deserializer.deserialize(rawConfig);
                listeners.get(configName).forEach(listener -> listener.configure(config));
            }
            curatorFramework.getData().usingWatcher(this).forPath(configName);
        }
    }
}
