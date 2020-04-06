package ru.vsu.strategies;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

public interface SendStrategy<K, V> {

    //void send(KafkaProducer<K, V> kafkaProducer, ProducerRecord<K, V> record) throws ExecutionException, InterruptedException;

    void send(Producer<K, V> kafkaProducer, Collection<ProducerRecord<K, V>> producerRecords) throws ExecutionException, InterruptedException;
}
