package ru.vsu;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import ru.vsu.clients.AbstractProducer;
import ru.vsu.clients.SimpleProducer;
import ru.vsu.shceduling.Task;
import ru.vsu.strategies.SimpleSendStrategy;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class Main {

    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(5);

        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties);

        AbstractProducer<String, String> myProducer = new SimpleProducer<>(
                kafkaProducer,
                new LinkedBlockingQueue<>(),
                new SimpleSendStrategy<>());


            executor.submit(new Task(myProducer));
            myProducer.send(new ProducerRecord<>("demo", LocalDateTime.now().toString()));

    }
    /*public static void main(String[] args) {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
             AdminClient adminClient = AdminClient.create(producerProperties)) {

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("testapp", LocalDateTime.now().toString());
            producer.send(producerRecord);

            List<ConfigResource> configResources = adminClient.describeCluster()
                    .nodes()
                    .get()
                    .stream()
                    .map(v -> new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(v.id())))
                    .collect(Collectors.toList());

            DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(configResources);
            List<String> config = describeConfigsResult.all().get()
                    .values()
                    .stream()
                    .map(v -> v.get("zookeeper.connect").value())
                    .collect(Collectors.toList());
            System.out.println(config);

            *//*try (ZooKeeper zooKeeper = new ZooKeeper("localhost:2181", 2000, null)) {
                //zooKeeper.create("/data", "some data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                byte[] data = zooKeeper.getData("/data", null, null);
                System.out.println(new String(data));
            }*//*
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }*/
}
