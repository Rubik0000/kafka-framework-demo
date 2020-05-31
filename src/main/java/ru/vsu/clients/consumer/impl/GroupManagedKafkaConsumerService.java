package ru.vsu.clients.consumer.impl;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import ru.vsu.clients.consumer.RecordListener;
import ru.vsu.clients.consumer.ConsumerService;
import ru.vsu.clients.consumer.impl.consumerthreads.AbstractConsumerThread;
import ru.vsu.clients.consumer.impl.consumerthreads.GroupManagedConsumerThread;
import ru.vsu.factories.consumers.original.OriginalConsumerFactory;
import ru.vsu.utils.Utils;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class GroupManagedKafkaConsumerService<K, V> extends AbstractConsumerService<K, V> implements ConsumerService<K, V> {

    /*private OriginalConsumerFactory<K, V> consumerFactory;
    private Map<String, Object> consumerConfig;*/
    private Map<String, List<AbstractConsumerThread<K, V>>> consumers;

    public GroupManagedKafkaConsumerService(OriginalConsumerFactory<K, V> consumerFactory, Map<String, Object> config) {
        super(consumerFactory, config);
    }

    public GroupManagedKafkaConsumerService(OriginalConsumerFactory<K, V> consumerFactory, Properties properties) {
        super(consumerFactory, properties);
    }


    /*public GroupManagedConsumerService(OriginalConsumerFactory<K, V> consumerFactory, Map<String, Object> config) {
        this.consumerFactory = consumerFactory;
        this.consumerConfig = new ConcurrentHashMap<>(config);
        consumers = new ConcurrentHashMap<>();
    }

    public GroupManagedConsumerService(OriginalConsumerFactory<K, V> consumerFactory, Properties properties) {
        this(consumerFactory, Utils.propertiesToMap(properties));
    }*/


    @Override
    public void subscribe(String topic, int levelOfPar, RecordListener<K, V> recordListener) {
        if (consumers.get(topic) != null) {
            consumers.get(topic).forEach(AbstractConsumerThread::close);
        }
        consumers.put(topic, new ArrayList<>());
        List<AbstractConsumerThread<K, V>> consumerThreads = consumers.get(topic);
        for (int i = 0; i < levelOfPar; ++i) {
            AbstractConsumerThread<K, V> consumerThread = new GroupManagedConsumerThread<>(
                    this,
                    topic,
                    recordListener,
                    String.format("thread-%s-%d", topic, i)
            );
            consumerThreads.add(consumerThread);
            consumerThread.start();
        }
    }

    @Override
    public void unsubscribe(String topic, RecordListener<K, V> recordListener) {
        List<AbstractConsumerThread<K, V>> consumerThreads = consumers.get(topic);
        if (consumerThreads != null) {
            Iterator<AbstractConsumerThread<K, V>> iterator = consumerThreads.iterator();
            while (iterator.hasNext()) {
                AbstractConsumerThread<K, V> consumerThread = iterator.next();
                if (consumerThread.getListener().equals(recordListener)) {
                    consumerThread.close();
                    iterator.remove();
                }
            }
        }
    }

    @Override
    public void unsubscribe(String topic) {
        List<AbstractConsumerThread<K, V>> consumerThreads = consumers.get(topic);
        if (consumerThreads != null) {
            consumerThreads.forEach(AbstractConsumerThread::close);
        }
    }

    @Override
    public void close() {
        consumers.forEach((k, v) -> unsubscribe(k));
    }

    @Override
    protected void configure() {
        consumers.keySet().forEach(topic -> {

        });
        consumers.forEach((s, consumerThreads) -> consumerThreads.forEach(AbstractConsumerThread::rerun));
    }


    /*class ConsumerThread extends Thread implements AutoCloseable {

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
            try (Consumer<K, V> kafkaConsumer = getConsumerFactory().createConsumer(getConsumerConfig())) {
                kafkaConsumer.subscribe(Collections.singletonList(topic));
                while (!isStopped) {
                    ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                    if (!consumerRecords.isEmpty()) {
                        System.out.println("Consumer thread: " + getName());
                        consumerRecords.forEach(listener::listen);
                        kafkaConsumer.commitSync();
                    }
                }
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
