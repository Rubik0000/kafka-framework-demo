package ru.vsu.clients.consumer.impl.consumerthreads;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import ru.vsu.clients.consumer.RecordListener;
import ru.vsu.clients.consumer.impl.AbstractConsumerService;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.*;

public class PartitionManagedConsumerThread<K, V> extends AbstractConsumerThread<K, V> {

    private final int[] partitions;
    private ExecutorService executorService;


    public PartitionManagedConsumerThread(
            AbstractConsumerService<K, V> consumerService,
            ExecutorService executorService,
            String topic,
            int[] partitions,
            RecordListener<K, V> recordListener,
            String threadName) {
        super(consumerService, topic, recordListener, threadName);
        this.partitions = partitions;
        this.executorService = executorService;
    }


    @Override
    public void run() {
        AbstractConsumerService<K, V> consumerService = getConsumerService();
        try (Consumer<K, V> kafkaConsumer = consumerService.getConsumerFactory().createConsumer(consumerService.getConsumerConfig())) {
            ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
            for (int i = 0; i < partitions.length; ++i) {
                topicPartitions.add(new TopicPartition(getTopic(), i));
            }
            kafkaConsumer.assign(topicPartitions);
            while (!isStopped()) {
                ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                if (!consumerRecords.isEmpty()) {
                    System.out.println("Consumer thread: " + getName());
                    consumerRecords.forEach(record -> executorService.submit(() -> getListener().listen(record)));
                    kafkaConsumer.commitSync();
                }
            }
        }
    }
}
