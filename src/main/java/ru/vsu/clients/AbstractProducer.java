package ru.vsu.clients;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import ru.vsu.shceduling.ScheduledTask;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public abstract class AbstractProducer<K, V> implements Producer<K, V>, ScheduledTask {

    private final KafkaProducer<K, V> kafkaProducer;
    private volatile boolean isRunning;


    public AbstractProducer(KafkaProducer<K, V> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
        isRunning = true;
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return kafkaProducer.partitionsFor(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return kafkaProducer.metrics();
    }

    @Override
    public void close(long timeout) {
        kafkaProducer.close(Duration.ofMillis(timeout));
    }

    @Override
    public void close() throws Exception {
        kafkaProducer.close();
    }

    @Override
    public boolean isCanceled() {
        return !isRunning;
    }

    protected final KafkaProducer<K, V> getKafkaProducer() {
        return kafkaProducer;
    }

    protected boolean isRunning() {
        return isRunning;
    }

    protected void setRunning(boolean running) {
        isRunning = running;
    }
}
