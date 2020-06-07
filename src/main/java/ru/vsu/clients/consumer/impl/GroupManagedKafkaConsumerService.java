package ru.vsu.clients.consumer.impl;

import ru.vsu.clients.consumer.ConsumerService;
import ru.vsu.clients.consumer.RecordListener;
import ru.vsu.clients.consumer.impl.consumerthreads.GroupManagedConsumerTask;
import ru.vsu.factories.consumers.original.OriginalConsumerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class GroupManagedKafkaConsumerService<K, V> extends AbstractConsumerService<K, V> implements ConsumerService<K, V> {

    private List<ScheduledFutureProxy<K, V>> tasks = new CopyOnWriteArrayList<>();
    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(5);


    public GroupManagedKafkaConsumerService(OriginalConsumerFactory<K, V> consumerFactory, Map<String, Object> config) {
        super(consumerFactory, config);
    }

    public GroupManagedKafkaConsumerService(OriginalConsumerFactory<K, V> consumerFactory, Properties properties) {
        super(consumerFactory, properties);
    }


    @Override
    public void subscribe(String topic, int levelOfPar, RecordListener<K, V> recordListener) {
        throwIfTopicIsNull(topic);
        throwIfLevelOfParallelismIfLessThanOne(levelOfPar);
        throwIfRecordListenerIsNull(recordListener);
        for (int i = 0; i < levelOfPar; ++i) {
            GroupManagedConsumerTask<K, V> consumerThread = new GroupManagedConsumerTask<>(
                    this,
                    topic,
                    recordListener,
                    String.format("thread-%s-%d", topic, i)
            );
            ScheduledFutureProxy<K, V> proxy = new ScheduledFutureProxy<>(
                    executorService.scheduleWithFixedDelay(consumerThread, 0, 10, TimeUnit.MILLISECONDS),
                    consumerThread
            );
            tasks.add(proxy);
        }
    }

    @Override
    public void unsubscribe(String topic, RecordListener<K, V> recordListener) {
        throwIfTopicIsNull(topic);
        throwIfRecordListenerIsNull(recordListener);
        List<ScheduledFutureProxy<K, V>> tasksToStop = tasks.stream()
                .filter(task -> topic.equals(task.getConsumerTask().getTopic()) && task.getConsumerTask().getListener().equals(recordListener))
                .collect(Collectors.toList());
        tasksToStop.forEach(task -> task.cancel(true));
        tasks.removeAll(tasksToStop);
    }

    @Override
    public void unsubscribe(String topic) {
        throwIfTopicIsNull(topic);
        List<ScheduledFutureProxy<K, V>> tasksToStop = tasks.stream()
                .filter(task -> topic.equals(task.getConsumerTask().getTopic()))
                .collect(Collectors.toList());
        tasksToStop.forEach(task -> task.cancel(true));
        tasks.removeAll(tasksToStop);
    }

    @Override
    public void close() {
        tasks.forEach(task -> task.cancel(true));
        executorService.shutdown();
    }

    @Override
    protected void configure() {
        tasks.forEach(s -> s.getConsumerTask().rerun());
    }
}
