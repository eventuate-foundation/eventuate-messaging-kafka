package io.eventuate.messaging.kafka.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.ConcurrentHashMap;

public class SwimlanePerTopicPartition implements TopicPartitionToSwimLaneMapping {

    private final ConcurrentHashMap<TopicPartition, Integer> mapping = new ConcurrentHashMap<>();

    @Override
    public Integer toSwimLane(TopicPartition topicPartition) {
        return mapping.computeIfAbsent(topicPartition, tp -> mapping.size());
    }
}
