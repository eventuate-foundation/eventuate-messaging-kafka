package io.eventuate.messaging.kafka.consumer;

import org.apache.kafka.common.TopicPartition;

public class OriginalTopicPartitionToSwimlaneMapping implements TopicPartitionToSwimlaneMapping {
    @Override
    public Integer toSwimlane(TopicPartition topicPartition, String messageKey) {
        return topicPartition.partition();
    }
}
