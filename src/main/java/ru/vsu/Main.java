package ru.vsu;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.vsu.clients.consumer.PartitionConsumerService;
import ru.vsu.clients.consumer.impl.PartitionManagedKafkaConsumerService;
import ru.vsu.factories.consumers.original.OriginalKafkaConsumerFactory;
import ru.vsu.kafkacache.Cache;
import ru.vsu.kafkacache.KafkaCache;

import java.util.Properties;
import java.util.UUID;

public class Main {

    public static void main(String[] args) throws Exception {
        //ExecutorService executor = Executors.newFixedThreadPool(5);

        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        producerProperties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        producerProperties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "180000");

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties);

        //ApacheDerbyDao apacheDerbyDao = new ApacheDerbyDao();
        //RocksDbDao rocksDbDao = new RocksDbDao(new JsonByteSerializer());


        /*KafkaProducerService<String, String> myProducer = new KafkaProducerService<>(
                new OriginalKafkaProducerFactory<>(),
                producerProperties,
                new SimpleSendStrategy<>(),
                new PersistentQueueStorageStrategy<>(rocksDbDao)
        );*/
       /* KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties);
        long start = System.currentTimeMillis();
        kafkaProducer.send(new ProducerRecord<>("demo", "test msg"), (metadata, exception) -> {
            exception.printStackTrace();
            System.out.println("Spend time :" + (System.currentTimeMillis() - start));
        });
        System.out.println("here");
        System.in.read();
        kafkaProducer.close(Duration.ofMillis(10000));*/
        /*PartitionConsumerService<String, String> consumerService = new PartitionManagedKafkaConsumerService<>(
                new OriginalKafkaConsumerFactory<>(),
                consumerProperties
        );

        consumerService.subscribe("test3", new int[] {0}, 3, r -> {
            System.out.println(String.format("Thread: %s, Partition: %d, Value: %s",
                    Thread.currentThread().getName(), r.partition(), r.value()));
        });*/
        Cache<String, String> cache = new KafkaCache<>(Serdes.String(), Serdes.String(), null);


        System.in.read();

        System.out.println("Get value:" + cache.get("key1"));

        cache.close();

        //consumerService.close();

        //myProducer.send(new ProducerRecord<>("demo", LocalDateTime.now().toString()));

        //myProducer.close(10000);
        //rocksDbDao.close();

//        kafkaProducer.send(new ProducerRecord<>("demo", LocalDateTime.now().toString()));
//        System.in.read();
//        kafkaProducer.close();

        //executor.submit(new Task(myProducer));

        //myProducer.close(10000);
        //executor.shutdown();

        /*myProducer.close(10000);
        apacheDerbyDao.close();*/

        /*RetryPolicy retryPolicy = new RetryNTimes(3, 100);
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

        System.in.read();*/

        //configurationService.close();
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
