package ru.vsu.clients.consumer.impl.consumerthreads;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import ru.vsu.clients.consumer.RecordListener;
import ru.vsu.clients.consumer.impl.AbstractConsumerService;

import java.time.Duration;
import java.util.Collections;

public class GroupManagedConsumerThread<K, V> extends AbstractConsumerThread<K, V> {

    public GroupManagedConsumerThread(AbstractConsumerService<K, V> consumerService, String topic, RecordListener<K, V> recordListener, String threadName) {
        super(consumerService, topic, recordListener, threadName);
    }

    @Override
    public void run() {
        AbstractConsumerService<K, V> consumerService = getConsumerService();
        try (Consumer<K, V> kafkaConsumer = consumerService.getConsumerFactory().createConsumer(consumerService.getConsumerConfig())) {
            kafkaConsumer.subscribe(Collections.singletonList(getTopic()));
            while (!isStopped()) {
                ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                if (!consumerRecords.isEmpty()) {
                    System.out.println("Consumer thread: " + getName());
                    consumerRecords.forEach(getListener()::listen);
                    kafkaConsumer.commitSync();
                }
            }
        }
    }
}
