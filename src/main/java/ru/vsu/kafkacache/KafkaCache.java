package ru.vsu.kafkacache;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serde;
import ru.vsu.clients.consumer.ConsumerService;
import ru.vsu.factories.consumers.consumerservice.ConsumerServiceFactory;
import ru.vsu.factories.consumers.consumerservice.KafkaConsumerServiceFactory;
import ru.vsu.factories.producers.original.OriginalKafkaProducerFactory;
import ru.vsu.factories.producers.original.OriginalProducerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class KafkaCache<K, V> implements Cache<K, V> {

    private ConsumerService<byte[], byte[]> consumerService;
    private Producer<byte[], byte[]> producer;
    private Map<K, V> localCache;
    private String topic;
    private String bootstrapServers;
    private Serde<K> keySerde;
    private Serde<V> valueSerde;
    private ConsumerServiceFactory consumerServiceFactory;
    private OriginalProducerFactory originalProducerFactory;


    public KafkaCache(Serde<K> keySerde, Serde<V> valueSerde, Properties properties) {
        this(keySerde, valueSerde, properties, new OriginalKafkaProducerFactory(), new KafkaConsumerServiceFactory());
    }

    KafkaCache(
            Serde<K> keySerde,
            Serde<V> valueSerde,
            Properties properties,
            OriginalProducerFactory originalProducerFactory,
            ConsumerServiceFactory consumerServiceFactory) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.consumerServiceFactory = consumerServiceFactory;
        this.originalProducerFactory = originalProducerFactory;
        this.topic = "cachetopic";
        this.bootstrapServers = "localhost:9092";
        this.originalProducerFactory = originalProducerFactory;
        this.producer = createProducer();
        this.consumerService = createConsumerService();
        this.consumerService.subscribe(topic, 1, this::listen);
        this.localCache = new ConcurrentHashMap<>();
    }


    @Override
    public void close() throws Exception {
        consumerService.close();
        producer.close();
    }

    @Override
    public int size() {
        return localCache.size();
    }

    @Override
    public boolean isEmpty() {
        return localCache.isEmpty();
    }

    @Override
    public boolean containsKey(Object o) {
        return localCache.containsKey(o);
    }

    @Override
    public boolean containsValue(Object o) {
        return localCache.containsValue(o);
    }

    @Override
    public V get(Object o) {
        return localCache.get(o);
    }

    @Override
    public V put(K k, V v) {
        try {
            producer.send(new ProducerRecord<>(
                    topic,
                    keySerde.serializer().serialize(topic, k),
                    v == null ? null : valueSerde.serializer().serialize(topic, v)
            )).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public V remove(Object o) {
        return put((K) o, null);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        map.forEach(this::put);
    }

    @Override
    public void clear() {

    }

    @Override
    public Set<K> keySet() {
        return null;
    }

    @Override
    public Collection<V> values() {
        return localCache.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return localCache.entrySet();
    }

    @Override
    public void resync() {
        try {
            consumerService.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        consumerService = createConsumerService();
    }

    private void listen(ConsumerRecord<byte[], byte[]> record) {
        K key = keySerde.deserializer().deserialize(topic, record.key());
        V value = valueSerde.deserializer().deserialize(topic, record.value());
        localCache.put(key, value);
        System.out.println("Get new key: " + key + " value: " + value);
    }

    private ConsumerService<byte[], byte[]> createConsumerService() {
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
        return consumerServiceFactory.createPartitionConsumerService(consumerProps);
    }

    private Producer<byte[], byte[]> createProducer() {
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        return originalProducerFactory.createProducer(producerProps);
    }
}
