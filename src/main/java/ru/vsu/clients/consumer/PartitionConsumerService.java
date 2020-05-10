package ru.vsu.clients.consumer;

public interface PartitionConsumerService<K, V> extends ConsumerService<K, V> {

    //void subscribeOnPartitions(Collection<TopicWithPartition> topicWithPartitions, RecordListener<K, V> recordListener);

    void subscribe(String topic, int[] partitions, int numberOfPar, RecordListener<K, V> recordListener);
}
