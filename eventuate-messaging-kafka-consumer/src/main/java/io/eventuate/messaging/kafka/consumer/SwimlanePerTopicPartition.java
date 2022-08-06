package io.eventuate.messaging.kafka.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.ConcurrentHashMap;

public class SwimlanePerTopicPartition implements TopicPartitionToSwimlaneMapping {

    private final ConcurrentHashMap<TopicPartition, Integer> mapping = new ConcurrentHashMap<>();

    @Override
    public Integer toSwimlane(TopicPartition topicPartition, String messageKey) {
        return mapping.computeIfAbsent(topicPartition, tp -> mapping.size());
    }
}
