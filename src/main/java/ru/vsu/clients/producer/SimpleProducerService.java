package ru.vsu.clients.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.KafkaThread;
import ru.vsu.configurationservices.ConfigurationListener;
import ru.vsu.factories.producers.original.OriginalProducerFactory;
import ru.vsu.strategies.send.SendStrategy;
import ru.vsu.strategies.storage.StorageStrategy;
import ru.vsu.utils.Utils;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class SimpleProducerService<K, V> implements ProducerService<K, V>, ConfigurationListener {

    //private final Queue<ProducerRecord<K, V>> queue;
    private final SendStrategy<K, V> sendStrategy;
    private final StorageStrategy<K, V> storageStrategy;
    private final OriginalProducerFactory<K, V> originalProducerFactory;
    private volatile boolean isRunning;
    private volatile boolean isReconfiguring;
    private volatile Producer<K, V> producer;
    private Thread senderThread;


    public SimpleProducerService(
            OriginalProducerFactory<K, V> originalProducerFactory,
            Map<String, Object> configs,
            SendStrategy<K, V> sendStrategy,
            StorageStrategy<K, V> storageStrategy) {
        this.originalProducerFactory = originalProducerFactory;
        this.producer = originalProducerFactory.createProducer(configs);
        this.isRunning = true;
        this.isReconfiguring = false;
        this.storageStrategy = storageStrategy;
        //this.queue = new LinkedBlockingDeque<>();
        this.sendStrategy = sendStrategy;
        senderThread = new KafkaThread("fuck", this::execute, true);
        senderThread.start();
    }

    public SimpleProducerService(
            OriginalProducerFactory<K, V> originalProducerFactory,
            Properties properties,
            SendStrategy<K, V> sendStrategy,
            StorageStrategy<K, V> storageStrategy) {
        this(originalProducerFactory, Utils.propertiesToMap(properties), sendStrategy, storageStrategy);
    }


    @Override
    public void send(ProducerRecord<K, V> record) {
        send(Collections.singletonList(record));
    }

    @Override
    public void send(Collection<ProducerRecord<K, V>> producerRecords) {
        if (isRunning) {
            storageStrategy.add(producerRecords);
            //queue.addAll(producerRecords);
        }
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return producer.partitionsFor(topic);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return producer.metrics();
    }

    @Override
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
    }

    @Override
    public void configure(Map<String, Object> configs) {
        isReconfiguring = true;
        producer.close();
        producer = originalProducerFactory.createProducer(configs);
        System.out.println("Producer has been reconfigured with " + configs);
        isReconfiguring = false;
    }

    protected void execute() {
        try {
            while (isRunning || !storageStrategy.isEmpty()) {
                Collection<ProducerRecord<K, V>> records = storageStrategy.get();
                if (!records.isEmpty() && !isReconfiguring) {
                    try {
                        sendStrategy.send(producer, records);
                        storageStrategy.getAndRemove();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }
}
