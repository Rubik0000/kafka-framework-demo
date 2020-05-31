package ru.vsu.clients.producer.api;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Collection;
import java.util.Map;

public interface ProducerService<K, V> extends AutoCloseable {

    void send(ProducerRecord<K, V> record, Callback callback);

    void send(Collection<ProducerRecord<K, V>> records, BatchCallback callback);

    void send(ProducerRecord<K, V> record);

    void send(Collection<ProducerRecord<K, V>> records);

    Map<MetricName, ? extends Metric> metrics();

    void close(long timeout);
}
