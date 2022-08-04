package io.eventuate.messaging.kafka.consumer;

import org.apache.kafka.common.TopicPartition;

public interface TopicPartitionToSwimLaneMapping {
    Integer toSwimLane(TopicPartition topicPartition);
}
