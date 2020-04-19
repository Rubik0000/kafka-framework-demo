package ru.vsu.clients.consumer;

public class TopicWithPartition {

    private String topic;
    private int[] partitions;


    public TopicWithPartition(String topic, int ...partitions) {
        this.topic = topic;
        this.partitions = partitions;
    }

    public String getTopic() {
        return topic;
    }

    public int[] getPartitions() {
        return partitions;
    }
}
