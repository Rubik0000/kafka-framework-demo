package ru.vsu.clients;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import ru.vsu.strategies.SendStrategy;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

public class SimpleProducer<K, V> extends AbstractProducer<K, V> {

    private final Queue<ProducerRecord<K, V>> queue;
    private final SendStrategy<K, V> sendStrategy;

    public SimpleProducer(
            KafkaProducer<K, V> kafkaProducer,
            Queue<ProducerRecord<K, V>> queue,
            SendStrategy<K, V> sendStrategy) {
        super(kafkaProducer);
        this.queue = queue;
        this.sendStrategy = sendStrategy;
    }

    @Override
    public void send(ProducerRecord<K, V> record) {
        queue.add(record);
    }

    @Override
    public void send(Collection<ProducerRecord<K, V>> producerRecords) {
        queue.addAll(producerRecords);
    }


    @Override
    public void execute() {
        ProducerRecord<K, V> record = null;
        try {
            record = queue.poll();
            if (record != null) {
                sendStrategy.send(getKafkaProducer(), record);
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            queue.add(record);
        }
    }
}
