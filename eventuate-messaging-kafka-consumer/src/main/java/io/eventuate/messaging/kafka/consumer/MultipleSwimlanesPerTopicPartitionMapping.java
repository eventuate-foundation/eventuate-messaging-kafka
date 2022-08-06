package io.eventuate.messaging.kafka.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.ConcurrentHashMap;

public class MultipleSwimlanesPerTopicPartitionMapping implements TopicPartitionToSwimlaneMapping {

    private final int swimlanesPerTopicPartition;

    private final ConcurrentHashMap<TopicPartition, Integer> mapping = new ConcurrentHashMap<>();

    public MultipleSwimlanesPerTopicPartitionMapping(int swimlanesPerTopicPartition) {
        this.swimlanesPerTopicPartition = swimlanesPerTopicPartition;
    }


    @Override
    public Integer toSwimlane(TopicPartition topicPartition, String messageKey) {
        int startingSwimlane = mapping.computeIfAbsent(topicPartition, tp -> mapping.size() * swimlanesPerTopicPartition);
        return startingSwimlane + messageKey.hashCode() % swimlanesPerTopicPartition;
    }
}
