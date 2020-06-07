package ru.vsu.clients.consumer.impl;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.utils.KafkaThread;
import ru.vsu.clients.consumer.PartitionConsumerService;
import ru.vsu.clients.consumer.RecordListener;
import ru.vsu.clients.consumer.impl.consumerthreads.PartitionManagedConsumerTask;
import ru.vsu.factories.consumers.original.OriginalConsumerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class PartitionManagedKafkaConsumerService<K, V> extends AbstractConsumerService<K, V> implements PartitionConsumerService<K, V> {

    private final Object locker = new Object();
    private final Thread pollThread = new KafkaThread("consumer-thread", this::execute, true);;
    private final AdminClient adminClient = KafkaAdminClient.create(getConsumerConfig());
    private final Map<String, List<PartitionManagedConsumerTask<K, V>>> tasks = new ConcurrentHashMap<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(6);
    private volatile boolean isRunning = true;
    private volatile boolean isReconfiguring = false;
    private Consumer<K, V> consumer = getConsumerFactory().createConsumer(getConsumerConfig());


    public PartitionManagedKafkaConsumerService(OriginalConsumerFactory<K, V> consumerFactory, Map<String, Object> config) {
        super(consumerFactory, config);
        pollThread.start();
    }

    public PartitionManagedKafkaConsumerService(OriginalConsumerFactory<K, V> consumerFactory, Properties properties) {
        super(consumerFactory, properties);
        pollThread.start();
    }


    @Override
    public void subscribe(String topic, int[] partitions, int numberOfPar, RecordListener<K, V> recordListener) {
        throwIfTopicIsNull(topic);
        throwIfLevelOfParallelismIfLessThanOne(numberOfPar);
        throwIfRecordListenerIsNull(recordListener);
        throwIfPartitionsAreNotPresent(partitions);
        tasks.putIfAbsent(topic, new CopyOnWriteArrayList<>());
        List<PartitionManagedConsumerTask<K, V>> topicTasks = tasks.get(topic);
        PartitionManagedConsumerTask<K, V> consumerTask = new PartitionManagedConsumerTask<>(
                partitions,
                numberOfPar,
                recordListener
        );
        topicTasks.add(consumerTask);
        Set<TopicPartition> assignment = new HashSet<>(consumer.assignment());
        for (int partition : partitions) {
            assignment.add(new TopicPartition(topic, partition));
        }
        synchronized (locker) {
            consumer.assign(assignment);
        }
    }

    @Override
    public void subscribe(String topic, int levelOfPar, RecordListener<K, V> recordListener) {
        throwIfTopicIsNull(topic);
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
        throwIfTopicIsNull(topic);
        throwIfRecordListenerIsNull(recordListener);
        List<PartitionManagedConsumerTask<K, V>> tasksToStop = tasks.get(topic)
                .stream()
                .filter(task -> task.getListener().equals(recordListener))
                .collect(Collectors.toList());
        tasks.get(topic).removeAll(tasksToStop);
    }

    @Override
    public void unsubscribe(String topic) {
        throwIfTopicIsNull(topic);
        tasks.remove(topic);
    }

    @Override
    public void close() throws Exception {
        try {
            isRunning = false;
            pollThread.join(10000);
            if (pollThread.isAlive()) {
                pollThread.interrupt();
                System.out.println("Interrupt");
            }
        } finally {
            tasks.clear();
            executorService.shutdown();
        }
    }

    @Override
    protected void configure() {
        synchronized (locker) {
            isReconfiguring = true;
            consumer.close();
            consumer = getConsumerFactory().createConsumer(getConsumerConfig());
            System.out.println("Consumer has been reconfigured with " + getConsumerConfig());
            isReconfiguring = false;
        }
    }

    private void execute() {
        while (isRunning) {
            if (isReconfiguring || consumer.assignment().isEmpty()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                continue;
            }
            ConsumerRecords<K, V> consumerRecords;
            synchronized (locker) {
                consumerRecords = consumer.poll(Duration.ofMillis(1000));
            }
            if (consumerRecords.isEmpty()) {
                continue;
            }
            tasks.forEach((key, value) -> {
                Iterable<ConsumerRecord<K, V>> records = consumerRecords.records(key);
                value.forEach(task -> {
                    Lists.partition(Lists.newArrayList(records), task.getLevelOfPar()).forEach(part -> {
                        executorService.submit(() -> task.run(part));
                    });
                });
            });
        }
    }

    protected void throwIfPartitionsAreNotPresent(int[] partitions) {
        if (partitions == null || partitions.length == 0) {
            throw new IllegalArgumentException("Partitions are not present");
        }
    }
}
