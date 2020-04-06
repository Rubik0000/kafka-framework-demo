package ru.vsu;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.vsu.clients.SimpleProducerService;
import ru.vsu.configurationservices.ConfigurationService;
import ru.vsu.configurationservices.deserializers.JsonDeserializer;
import ru.vsu.configurationservices.zookeeper.ZookeeperConfigurationService;
import ru.vsu.factories.producers.original.OriginalKafkaProducerFactory;
import ru.vsu.strategies.send.SimpleSendStrategy;
import ru.vsu.strategies.storage.StoreByOneStrategy;
import ru.vsu.utils.Utils;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(5);

        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        //KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties);

        SimpleProducerService<String, String> myProducer = new SimpleProducerService<>(
                new OriginalKafkaProducerFactory<>(),
                producerProperties,
                new SimpleSendStrategy<>(),
                new StoreByOneStrategy<>()
        );

//        kafkaProducer.send(new ProducerRecord<>("demo", LocalDateTime.now().toString()));
//        System.in.read();
//        kafkaProducer.close();

        //executor.submit(new Task(myProducer));

        //myProducer.close(10000);
        //executor.shutdown();


        RetryPolicy retryPolicy = new RetryNTimes(3, 100);
        CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181", retryPolicy);
        client.start();


        if (client.checkExists().forPath(ZookeeperConfigurationService.CONFIG_STORAGE_PATH + "myconf") == null) {
            client.create().creatingParentsIfNeeded().forPath(ZookeeperConfigurationService.CONFIG_STORAGE_PATH + "myconf");
        }

        ConfigurationService configurationService = new ZookeeperConfigurationService(client, new JsonDeserializer());
        configurationService.registerListener("myconf", myProducer);


        Random r = new Random();
        for (int i = 0; i < 3; ++i) {
            byte[] bytes = new ObjectMapper().writeValueAsBytes(Utils.propertiesToMap(producerProperties));
            client.setData().forPath(ZookeeperConfigurationService.CONFIG_STORAGE_PATH + "myconf", bytes);
            Thread.sleep(2000);
        }
        myProducer.send(new ProducerRecord<>("demo", LocalDateTime.now().toString()));

        System.in.read();

        myProducer.close();
        configurationService.close();
        /*client.getData().usingWatcher(new CuratorWatcher() {
            @Override
            public void process(WatchedEvent event) throws Exception {
                System.out.println("Event appears on" + event.getPath() + ":" + new String(client.getData().forPath(event.getPath())));
                client.getData().usingWatcher(this).forPath("/testapp");
            }
        }).forPath("/testapp");*/


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
