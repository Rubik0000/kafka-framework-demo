package ru.vsu.clients.consumer.impl.consumerthreads;

import ru.vsu.clients.consumer.RecordListener;
import ru.vsu.clients.consumer.impl.AbstractConsumerService;

public abstract class AbstractConsumerThread<K, V> extends Thread implements AutoCloseable {

    private final RecordListener<K, V> listener;
    private final String topic;
    private volatile boolean stopped = false;
    private final AbstractConsumerService<K, V> consumerService;

    public AbstractConsumerThread(AbstractConsumerService<K, V> consumerService, String topic, RecordListener<K, V> recordListener, String threadName) {
        setName(threadName);
        this.consumerService = consumerService;
        this.topic = topic;
        this.listener = recordListener;
    }


    @Override
    public void close() {
        setStopped(true);
    }

    public void rerun() {
        setStopped(true);
        while (isAlive()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        setStopped(false);
        start();
    }

    public AbstractConsumerService<K, V> getConsumerService() {
        return consumerService;
    }

    public RecordListener<K, V> getListener() {
        return listener;
    }

    public String getTopic() {
        return topic;
    }

    protected boolean isStopped() {
        return stopped;
    }

    protected void setStopped(boolean stopped) {
        this.stopped = stopped;
    }
}
