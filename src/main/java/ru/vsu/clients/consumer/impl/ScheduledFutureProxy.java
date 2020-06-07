package ru.vsu.clients.consumer.impl;

import ru.vsu.clients.consumer.impl.consumerthreads.GroupManagedConsumerTask;

import java.util.concurrent.*;

public class ScheduledFutureProxy<K, V> implements ScheduledFuture<Object> {

    private ScheduledFuture<?> proxy;
    private final GroupManagedConsumerTask<K, V> consumerTask;


    public ScheduledFutureProxy(ScheduledFuture<?> proxy, GroupManagedConsumerTask<K, V> consumerTask) {
        this.proxy = proxy;
        this.consumerTask = consumerTask;
    }


    @Override
    public long getDelay(TimeUnit timeUnit) {
        return proxy.getDelay(timeUnit);
    }

    @Override
    public int compareTo(Delayed delayed) {
        return proxy.compareTo(delayed);
    }

    @Override
    public boolean cancel(boolean b) {
        try {
            consumerTask.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return proxy.cancel(b);
    }

    @Override
    public boolean isCancelled() {
        return proxy.isCancelled();
    }

    @Override
    public boolean isDone() {
        return proxy.isDone();
    }

    @Override
    public Object get() throws InterruptedException, ExecutionException {
        return proxy.get();
    }

    @Override
    public Object get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
        return proxy.get(l, timeUnit);
    }

    public GroupManagedConsumerTask<K, V> getConsumerTask() {
        return consumerTask;
    }
}
