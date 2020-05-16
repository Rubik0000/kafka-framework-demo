package ru.vsu.strategies.send;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

public class SimpleSendStrategy<K, V> implements SendStrategy<K, V> {

    @Override
    public void send(Producer<K, V> kafkaProducer, Collection<ProducerRecord<K, V>> producerRecords, SendStrategyCallback<K, V> callback) {
        for (ProducerRecord<K, V> record : producerRecords) {
            try {
                kafkaProducer.send(record, (metadata, exception) -> {
                    callback.onCompletion(record,  metadata, exception);
                }).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
