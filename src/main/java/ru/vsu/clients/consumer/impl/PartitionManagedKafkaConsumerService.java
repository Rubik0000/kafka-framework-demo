package ru.vsu.clients.consumer.impl;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import ru.vsu.clients.consumer.RecordListener;
import ru.vsu.clients.consumer.PartitionConsumerService;
import ru.vsu.clients.consumer.impl.consumerthreads.AbstractConsumerThread;
import ru.vsu.clients.consumer.impl.consumerthreads.PartitionManagedConsumerThread;
import ru.vsu.factories.consumers.original.OriginalConsumerFactory;
import ru.vsu.utils.Utils;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.IntFunction;

public class PartitionManagedKafkaConsumerService<K, V> extends AbstractConsumerService<K, V> implements PartitionConsumerService<K, V> {

    /*private OriginalConsumerFactory<K, V> consumerFactory;
    private Map<String, Object> consumerConfig;*/
    private Map<String, Pair<ExecutorService, AbstractConsumerThread<K, V>>> consumers;
    private AdminClient adminClient;
    private Map<Integer, ExecutorService> executorServices;


    public PartitionManagedKafkaConsumerService(OriginalConsumerFactory<K, V> consumerFactory, Map<String, Object> config) {
        super(consumerFactory, config);
        adminClient = KafkaAdminClient.create(config);
        consumers = new ConcurrentHashMap<>();
    }

    public PartitionManagedKafkaConsumerService(OriginalConsumerFactory<K, V> consumerFactory, Properties properties) {
        super(consumerFactory, properties);
        adminClient = KafkaAdminClient.create(properties);
        consumers = new ConcurrentHashMap<>();
    }

    /*public PartitionManagedConsumerService(OriginalConsumerFactory<K, V> consumerFactory, Map<String, Object> config) {
        this.consumerFactory = consumerFactory;
        this.consumerConfig = new ConcurrentHashMap<>(config);
        consumers = new ConcurrentHashMap<>();
    }

    public PartitionManagedConsumerService(OriginalConsumerFactory<K, V> consumerFactory, Properties properties) {
        this(consumerFactory, Utils.propertiesToMap(properties));
    }*/


    @Override
    public void subscribe(String topic, int[] partitions, int numberOfPar, RecordListener<K, V> recordListener) {
        if (consumers.get(topic) != null) {
            consumers.get(topic).getKey().shutdown();
            consumers.get(topic).getValue().close();
        }
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfPar);
        AbstractConsumerThread<K, V> consumerThread = new PartitionManagedConsumerThread<>(
                this,
                executorService,
                topic,
                partitions,
                recordListener,
                String.format("thread-%s", topic));
        consumers.put(topic, Pair.of(executorService, consumerThread));
        consumerThread.start();
    }

    @Override
    public void subscribe(String topic, int levelOfPar, RecordListener<K, V> recordListener) {
        adminClient.describeTopics(Collections.singletonList(topic)).all().whenComplete((stringTopicDescriptionMap, throwable) -> {
            if (throwable != null) {
                throw new RuntimeException(throwable);
            }
            List<TopicPartitionInfo> partitions = stringTopicDescriptionMap.get(topic).partitions();
            int[] partitionsArray = new int[partitions.size()];
            for (int i = 0; i < partitionsArray.length; ++i) {
                partitionsArray[i] = partitions.get(i).partition();
            }
            subscribe(topic, partitionsArray, levelOfPar, recordListener);
        });
    }

    @Override
    public void unsubscribe(String topic, RecordListener<K, V> recordListener) {

    }

    @Override
    public void unsubscribe(String topic) {

    }

    @Override
    public void close() throws Exception {
        consumers.forEach((k, v) -> {
            v.getKey().shutdown();
            v.getValue().close();
        });
    }

    @Override
    protected void configure() {
        consumers.forEach((k, v) -> v.getValue().rerun());
    }

    /*class ConsumerThread extends Thread implements AutoCloseable {

        private final RecordListener<K, V> listener;
        private final String topic;
        private int[] partitions;
        private volatile boolean isStopped = false;
        private ExecutorService executorService;


        public ConsumerThread(String topic, int[] partitions, int numberOfPar, RecordListener<K, V> recordListener, String threadName) {
            setName(threadName);
            this.topic = topic;
            this.listener = recordListener;
            this.partitions = partitions;
            executorService = new ThreadPoolExecutor(numberOfPar, numberOfPar, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000));
        }

        @Override
        public void run() {
            try (Consumer<K, V> kafkaConsumer = getConsumerFactory().createConsumer(getConsumerConfig())) {
                ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
                for (int i = 0; i < partitions.length; ++i) {
                    topicPartitions.add(new TopicPartition(topic, i));
                }
                kafkaConsumer.assign(topicPartitions);
                while (!isStopped) {
                    ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                    if (!consumerRecords.isEmpty()) {
                        System.out.println("Consumer thread: " + getName());
                        consumerRecords.forEach(record -> executorService.submit(() -> listener.listen(record)));
                        kafkaConsumer.commitSync();
                    }
                }
                executorService.shutdown();
            }
        }

        @Override
        public void close() {
            isStopped = true;
        }

        public void rerun() {
            close();
            while (isAlive()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            isStopped = false;
            start();
        }
    }*/
}
