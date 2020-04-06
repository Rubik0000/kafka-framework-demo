package ru.vsu.clients;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.KafkaThread;
import ru.vsu.factories.producers.original.OriginalProducerFactory;
import ru.vsu.strategies.SendStrategy;
import ru.vsu.utils.Utils;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class SimpleMyProducer<K, V> implements MyProducer<K, V> {

    private final Queue<ProducerRecord<K, V>> queue;
    private final SendStrategy<K, V> sendStrategy;
    private final OriginalProducerFactory<K, V> originalProducerFactory;
    private volatile boolean isRunning;
    private volatile boolean forceStop;
    private volatile Producer<K, V> producer;
    private volatile long idleTimeout;
    private volatile long lastCallTime;
    private Thread senderThread;


    public SimpleMyProducer(
            OriginalProducerFactory<K, V> originalProducerFactory,
            Map<String, Object> configs,
            Queue<ProducerRecord<K, V>> queue,
            SendStrategy<K, V> sendStrategy,
            long idleTimeout) {
        this.originalProducerFactory = originalProducerFactory;
        this.producer = originalProducerFactory.createProducer(configs);
        this.isRunning = false;
        this.queue = queue;
        this.sendStrategy = sendStrategy;
        this.idleTimeout = idleTimeout;

//        senderThread = new KafkaThread("fuck", this::execute, false);
//        senderThread.start();
    }

    public SimpleMyProducer(
            OriginalProducerFactory<K, V> originalProducerFactory,
            Properties properties,
            Queue<ProducerRecord<K, V>> queue,
            SendStrategy<K, V> sendStrategy,
            long idleTimeout) {
        this(originalProducerFactory, Utils.propertiesToMap(properties), queue, sendStrategy, idleTimeout);
    }


    @Override
    public void send(ProducerRecord<K, V> record) {
        send(Collections.singletonList(record));
    }

    @Override
    public void send(Collection<ProducerRecord<K, V>> producerRecords) {
        lastCallTime = System.currentTimeMillis();
        if (!isRunning) {
            senderThread = new KafkaThread("fuck", this::execute, false);
            senderThread.start();
            isRunning = true;
        }
        queue.addAll(producerRecords);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return producer.partitionsFor(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return producer.metrics();
    }

    /*@Override
    public void close(long timeout) {
        try {
            isRunning = false;
            senderThread.join(timeout * 3 / 4);
            if (senderThread.isAlive()) {
                senderThread.interrupt();
                System.out.println("Interrupt");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close(Duration.ofMillis(timeout / 4));
            System.out.println("The producer was closed");
        }
    }

    @Override
    public void close() throws Exception {
        close(Long.MAX_VALUE);
    }*/

    protected void execute() {
        try {
            System.out.println("Start demon");
            while ((System.currentTimeMillis() - lastCallTime) < idleTimeout) {
                ProducerRecord<K, V> record = queue.peek();
                if (record != null) {
                    sendStrategy.send(producer, Collections.singletonList(record));
                    queue.poll();
                }
            }
            producer.close();
            isRunning = false;
            System.out.println("Stop demon");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
