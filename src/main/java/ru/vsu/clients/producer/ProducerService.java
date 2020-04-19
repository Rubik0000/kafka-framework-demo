package ru.vsu.clients.producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface ProducerService<K, V> extends AutoCloseable {

    void send(ProducerRecord<K, V> record);

    void send(Collection<ProducerRecord<K, V>> records);

    //Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback);

    List<PartitionInfo> partitionsFor(String topic);

    Map<MetricName, ? extends Metric> metrics();

    void close(long timeout);
}
