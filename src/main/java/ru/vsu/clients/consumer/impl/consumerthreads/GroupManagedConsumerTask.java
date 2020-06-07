package ru.vsu.clients.consumer.impl.consumerthreads;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import ru.vsu.clients.consumer.RecordListener;
import ru.vsu.clients.consumer.impl.AbstractConsumerService;

import java.time.Duration;
import java.util.Collections;

public class GroupManagedConsumerTask<K, V>  implements Runnable, AutoCloseable {

    private Consumer<K, V> kafkaConsumer;
    private volatile boolean closed = false;
    private volatile boolean rerun = false;
    private final AbstractConsumerService<K, V> consumerService;
    private final RecordListener<K, V> listener;
    private final String topic;

    public GroupManagedConsumerTask(AbstractConsumerService<K, V> consumerService, String topic, RecordListener<K, V> recordListener, String threadName) {
        this.topic = topic;
        this.listener = recordListener;
        this.consumerService = consumerService;
        kafkaConsumer = consumerService.getConsumerFactory().createConsumer(consumerService.getConsumerConfig());
        kafkaConsumer.subscribe(Collections.singletonList(getTopic()));
    }

    @Override
    public void run() {
        if (!closed) {
            ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
            if (!consumerRecords.isEmpty()) {
                consumerRecords.forEach(getListener()::listen);
                kafkaConsumer.commitSync();
            }
            if (closed) {
                kafkaConsumer.close();
            }
            if (rerun) {
                kafkaConsumer.close();
                kafkaConsumer = consumerService.getConsumerFactory().createConsumer(consumerService.getConsumerConfig());
                kafkaConsumer.subscribe(Collections.singletonList(getTopic()));
                rerun = false;
            }
        } else {
            kafkaConsumer.close();
        }
    }

    public void rerun() {
        rerun = true;
    }

    public void close() throws Exception {
        //kafkaConsumer.close();
        closed = true;
    }

    public RecordListener<K, V> getListener() {
        return listener;
    }

    public String getTopic() {
        return topic;
    }
}
