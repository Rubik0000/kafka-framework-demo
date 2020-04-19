package ru.vsu.clients.consumer.impl;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import ru.vsu.clients.consumer.RecordListener;
import ru.vsu.clients.consumer.SubscribeConsumerService;
import ru.vsu.factories.consumers.original.OriginalConsumerFactory;
import ru.vsu.utils.Utils;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class GroupManagedSubscribeConsumerService<K, V> implements SubscribeConsumerService<K, V> {

    private OriginalConsumerFactory<K, V> consumerFactory;
    private Map<String, Object> consumerConfig;
    private Map<String, List<ConsumerThread>> consumers;


    public GroupManagedSubscribeConsumerService(OriginalConsumerFactory<K, V> consumerFactory, Map<String, Object> config) {
        this.consumerFactory = consumerFactory;
        this.consumerConfig = new ConcurrentHashMap<>(config);
        consumers = new ConcurrentHashMap<>();
    }

    public GroupManagedSubscribeConsumerService(OriginalConsumerFactory<K, V> consumerFactory, Properties properties) {
        this(consumerFactory, Utils.propertiesToMap(properties));
    }


    @Override
    public void subscribe(String topic, int numberOfPar, RecordListener<K, V> recordListener) {
        if (consumers.get(topic) != null) {
            consumers.get(topic).forEach(ConsumerThread::close);
        }
        consumers.put(topic, new ArrayList<>());
        List<ConsumerThread> consumerThreads = consumers.get(topic);
        for (int i = 0; i < numberOfPar; ++i) {
            ConsumerThread consumerThread = new ConsumerThread(topic, recordListener, String.format("thread-%s-%d", topic, i));
            consumerThreads.add(consumerThread);
            consumerThread.start();
        }
    }

    @Override
    public void close() {
        consumers.forEach((k, v) -> v.forEach(ConsumerThread::close));
    }


    class ConsumerThread extends Thread implements AutoCloseable {

        private final RecordListener<K, V> listener;
        private final String topic;
        private volatile boolean isStopped = false;


        public ConsumerThread(String topic, RecordListener<K, V> recordListener, String threadName) {
            setName(threadName);
            this.topic = topic;
            this.listener = recordListener;
        }

        @Override
        public void run() {
            try (Consumer<K, V> kafkaConsumer = consumerFactory.createConsumer(consumerConfig)) {
                kafkaConsumer.subscribe(Collections.singletonList(topic));
                while (!isStopped) {
                    ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                    if (!consumerRecords.isEmpty()) {
                        System.out.println("Consumer thread: " + getName());
                        listener.listen(consumerRecords);
                        kafkaConsumer.commitSync();
                    }
                }
            }
        }

        @Override
        public void close() {
            isStopped = true;
        }
    }
}
