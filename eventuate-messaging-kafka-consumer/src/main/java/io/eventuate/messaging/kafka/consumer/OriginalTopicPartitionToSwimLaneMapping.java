package io.eventuate.messaging.kafka.consumer;

import org.apache.kafka.common.TopicPartition;

public class OriginalTopicPartitionToSwimLaneMapping implements TopicPartitionToSwimLaneMapping {
    @Override
    public Integer toSwimLane(TopicPartition topicPartition, String messageKey) {
        return topicPartition.partition();
    }
}
