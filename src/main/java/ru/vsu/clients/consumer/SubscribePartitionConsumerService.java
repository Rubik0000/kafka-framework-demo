package ru.vsu.clients.consumer;

import java.util.Collection;
import java.util.Collections;

public interface SubscribePartitionConsumerService<K, V> extends SubscribeConsumerService<K, V> {

    //void subscribeOnPartitions(Collection<TopicWithPartition> topicWithPartitions, RecordListener<K, V> recordListener);

    void subscribe(String topic, int[] partitions, int numberOfPar, RecordListener<K, V> recordListener);
}
