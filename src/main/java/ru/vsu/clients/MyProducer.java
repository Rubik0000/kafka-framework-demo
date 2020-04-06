package ru.vsu.clients;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public interface MyProducer<K, V> /*extends AutoCloseable*/ {

    void send(ProducerRecord<K, V> record);

    void send(Collection<ProducerRecord<K, V>> records);

    //Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback);

    List<PartitionInfo> partitionsFor(String topic);

    Map<MetricName, ? extends Metric> metrics();

    //void close(long timeout);
}
