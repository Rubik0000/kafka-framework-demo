package ru.vsu;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.zookeeper.ZooKeeper;
import ru.vsu.clients.AbstractMyProducer;
import ru.vsu.clients.MyProducer;
import ru.vsu.clients.SimpleMyProducer;
import ru.vsu.factories.producers.original.OriginalKafkaProducerFactory;
import ru.vsu.shceduling.Task;
import ru.vsu.strategies.SimpleSendStrategy;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.*;

public class Main {

    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(5);

        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties);

        MyProducer<String, String> myProducer = new SimpleMyProducer<>(
                new OriginalKafkaProducerFactory<>(),
                producerProperties,
                new LinkedBlockingQueue<>(),
                new SimpleSendStrategy<>(),
                5000
        );

        //executor.submit(new Task(myProducer));
        myProducer.send(new ProducerRecord<>("demo", LocalDateTime.now().toString()));
        ZooKeeper zooKeeper = new ZooKeeper("localhost:2181", 2000, null);
        //zooKeeper.addWatch();
        //myProducer.close(10000);
        //executor.shutdown();

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
