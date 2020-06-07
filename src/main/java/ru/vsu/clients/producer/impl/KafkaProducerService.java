package ru.vsu.clients.producer.impl;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.utils.KafkaThread;
import ru.vsu.clients.producer.api.BatchCallback;
import ru.vsu.clients.producer.api.ProducerService;
import ru.vsu.configurationservices.api.ConfigurationListener;
import ru.vsu.factories.producers.original.OriginalProducerFactory;
import ru.vsu.strategies.send.SendStrategy;
import ru.vsu.strategies.send.exception.ProducerIsNeededToRecreate;
import ru.vsu.strategies.storage.QueueStorageStrategy;
import ru.vsu.utils.Utils;

import java.time.Duration;
import java.util.*;

public class KafkaProducerService<K, V> implements ProducerService<K, V>, ConfigurationListener {

    private static final BatchCallback BATCH_CALLBACK_MOCK = (metadata, exception, numberInBatch, batchCount) -> { };


    private final SendStrategy<K, V> sendStrategy;
    private final QueueStorageStrategy<K, V> queueStorageStrategy;
    private final OriginalProducerFactory originalProducerFactory;
    private final Object locker = new Object();
    private volatile boolean isRunning;
    private volatile boolean isReconfiguring;
    private volatile Producer<K, V> producer;
    private Map<String, Object> configs;
    private Thread senderThread;


    public KafkaProducerService(
            OriginalProducerFactory originalProducerFactory,
            Map<String, Object> configs,
            SendStrategy<K, V> sendStrategy,
            QueueStorageStrategy<K, V> queueStorageStrategy) {
        this.originalProducerFactory = originalProducerFactory;
        this.configs = configs;
        this.producer = originalProducerFactory.createProducer(configs);
        this.isRunning = true;
        this.isReconfiguring = false;
        this.queueStorageStrategy = queueStorageStrategy;
        this.sendStrategy = sendStrategy;
        senderThread = new KafkaThread("producer-thread", this::execute, true);
        senderThread.start();
    }

    public KafkaProducerService(
            OriginalProducerFactory originalProducerFactory,
            Properties properties,
            SendStrategy<K, V> sendStrategy,
            QueueStorageStrategy<K, V> queueStorageStrategy) {
        this(originalProducerFactory, Utils.propertiesToMap(properties), sendStrategy, queueStorageStrategy);
    }


    @Override
    public void send(ProducerRecord<K, V> record, Callback callback) {
        send(
                Collections.singletonList(record),
                callback == null
                        ? BATCH_CALLBACK_MOCK
                        : (metadata, exception, numberInBatch, batchCount) -> callback.onCompletion(metadata, exception)
        );
    }

    @Override
    public void send(Collection<ProducerRecord<K, V>> producerRecords, BatchCallback callback) {
        if (isRunning) {
            int batchSize = producerRecords.size();
            List<ProducerRecord<K, V>> oldRecords = new ArrayList<>(producerRecords);
            Collection<ProducerRecord<K, V>> newRecords = new ArrayList<>();
            for (int i = 0; i < batchSize; ++i) {
                newRecords.add(new ProducerRecordWithCallback<>(
                        oldRecords.get(i),
                        new BatchCallbackWrapper(i, batchSize, callback == null ? BATCH_CALLBACK_MOCK : callback)
                ));
            }
            queueStorageStrategy.add(newRecords);
        }
    }

    @Override
    public void send(ProducerRecord<K, V> record) {
        send(record, null);
    }

    @Override
    public void send(Collection<ProducerRecord<K, V>> producerRecords) {
        send(producerRecords, null);
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
        synchronized (locker) {
            isReconfiguring = true;
            producer.close();
            producer = originalProducerFactory.createProducer(configs);
            System.out.println("Producer has been reconfigured with " + configs);
            isReconfiguring = false;
        }
    }

    private void execute() {
        while (isRunning || !queueStorageStrategy.isEmpty()) {
            Collection<ProducerRecord<K, V>> records = queueStorageStrategy.get();
            if (!records.isEmpty() && !isReconfiguring) {
                try {
                    sendStrategy.send(producer, records, (producerRecord, metadata, exception) -> {
                        ProducerRecordWithCallback<K, V> producerRecordWithCallback = (ProducerRecordWithCallback<K, V>) producerRecord;
                        producerRecordWithCallback.callback().onCompletion(metadata, exception);
                    });
                    queueStorageStrategy.getAndRemove();
                } catch (ProducerIsNeededToRecreate producerIsNeededToRecreate) {
                    producerIsNeededToRecreate.printStackTrace();
                    configure(configs);
                }
            }
        }
    }
}
