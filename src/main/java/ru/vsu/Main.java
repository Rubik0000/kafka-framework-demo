package ru.vsu;

import com.sun.tools.javac.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.vsu.clients.consumer.ConsumerService;
import ru.vsu.clients.consumer.PartitionConsumerService;
import ru.vsu.clients.consumer.RecordListener;
import ru.vsu.clients.consumer.impl.GroupManagedKafkaConsumerService;
import ru.vsu.clients.consumer.impl.PartitionManagedKafkaConsumerService;
import ru.vsu.clients.producer.api.ProducerService;
import ru.vsu.clients.producer.impl.KafkaProducerService;
import ru.vsu.configurationservices.api.ConfigurationListener;
import ru.vsu.configurationservices.api.ConfigurationService;
import ru.vsu.configurationservices.impl.KafkaConfigurationService;
import ru.vsu.dao.RocksDbDao;
import ru.vsu.dao.StoredProducerRecordsDao;
import ru.vsu.dao.serialization.serializers.JsonByteSerializer;
import ru.vsu.factories.consumers.original.OriginalKafkaConsumerFactory;
import ru.vsu.factories.producers.original.OriginalKafkaProducerFactory;
import ru.vsu.strategies.send.SimpleSendStrategy;
import ru.vsu.strategies.storage.PersistentQueueStorageStrategy;
import ru.vsu.strategies.storage.QueueStorageStrategy;
import ru.vsu.strategies.storage.StoreByOneQueueStorageStrategy;

import java.util.*;

public class Main {

    public static void main(String[] args) throws Exception {
        //ExecutorService executor = Executors.newFixedThreadPool(5);

        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        producerProperties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
//        producerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
//        producerProperties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "180000");

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ArrayList<ProducerRecord<String, String>> records = new ArrayList<>(1_000_0000);
        Collections.fill(records, new ProducerRecord<>("test_stress", "test"));


        try (RocksDbDao dao = new RocksDbDao(new JsonByteSerializer())) {
            //System.in.read();
            //System.out.println("Start stress test");
            QueueStorageStrategy<String, String> queueStorageStrategy = new PersistentQueueStorageStrategy<>(dao);
            try (ProducerService<String, String> producerService = new KafkaProducerService<String, String>(
                    new OriginalKafkaProducerFactory(),
                    producerProperties,
                    new SimpleSendStrategy<>(),
                    queueStorageStrategy
            )) {
                int counter = Integer.parseInt(args[0]);
                while (true) {
                    String message = "message " + counter++;
                    producerService.send(new ProducerRecord<>("demo", message));
                    System.out.println("Send " + message);
                    Thread.sleep(5000);
                }
            }
        }

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
