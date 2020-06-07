package ru.vsu.clients.consumer.impl.consumerthreads;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import ru.vsu.clients.consumer.RecordListener;

import java.util.Collection;

public class PartitionManagedConsumerTask<K, V>  {

    private final int[] partitions;
    private final int levelOfPar;
    private final RecordListener<K, V> listener;


    public PartitionManagedConsumerTask(
            int[] partitions,
            int levelOfPar,
            RecordListener<K, V> recordListener) {
        this.partitions = partitions;
        this.levelOfPar = levelOfPar;
        this.listener = recordListener;
    }


    public void run(Collection<ConsumerRecord<K, V>> records) {
        records.forEach(getListener()::listen);
    }

    public int getLevelOfPar() {
        return levelOfPar;
    }

    public int[] getPartitions() {
        return partitions;
    }

    public RecordListener<K, V> getListener() {
        return listener;
    }
}
