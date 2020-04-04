package ru.vsu.strategies;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

public interface SendStrategy<K, V> {

    void send(KafkaProducer<K, V> kafkaProducer, ProducerRecord<K, V> record) throws ExecutionException, InterruptedException;

    void send(KafkaProducer<K, V> kafkaProducer, Collection<ProducerRecord<K, V>> producerRecords) throws ExecutionException, InterruptedException;
}
