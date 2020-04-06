package ru.vsu.clients;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import ru.vsu.factories.producers.original.OriginalProducerFactory;
import ru.vsu.shceduling.ScheduledTask;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class AbstractMyProducer<K, V> implements MyProducer<K, V> {

    private final OriginalProducerFactory<K, V> originalProducerFactory;
    private volatile boolean isRunning;
    private volatile Producer<K, V> producer;


    public AbstractMyProducer(OriginalProducerFactory<K, V> originalProducerFactory, Map<String, Object> configs) {
        this.originalProducerFactory = originalProducerFactory;
        this.producer = originalProducerFactory.createProducer(configs);
        this.isRunning = true;
    }

    public AbstractMyProducer(OriginalProducerFactory<K, V> originalProducerFactory, Properties properties) {
        this.originalProducerFactory = originalProducerFactory;
        this.producer = originalProducerFactory.createProducer(properties);
        this.isRunning = true;
    }


    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return producer.partitionsFor(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return producer.metrics();
    }


    protected final OriginalProducerFactory<K, V> getOriginalProducerFactory() {
        return originalProducerFactory;
    }

    protected boolean isRunning() {
        return isRunning;
    }

    protected void setRunning(boolean running) {
        isRunning = running;
    }

    protected Producer<K, V> getProducer() {
        return producer;
    }

    protected void setProducer(Producer<K, V> producer) {
        this.producer = producer;
    }


}
