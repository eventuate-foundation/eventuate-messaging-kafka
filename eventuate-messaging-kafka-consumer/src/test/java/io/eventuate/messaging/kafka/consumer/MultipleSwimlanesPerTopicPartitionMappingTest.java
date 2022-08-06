package io.eventuate.messaging.kafka.consumer;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MultipleSwimlanesPerTopicPartitionMappingTest {

    @Test
    public void shouldMap() {
        int swimlanesPerTopicPartition = 4;

        MultipleSwimlanesPerTopicPartitionMapping mapping = new MultipleSwimlanesPerTopicPartitionMapping(swimlanesPerTopicPartition);

        assertSwimlaneWithinRange(mapping, tp0(), "X", 0, swimlanesPerTopicPartition);
        assertSwimlaneWithinRange(mapping, tp0(), "Y", 0, swimlanesPerTopicPartition);
        assertSwimlaneWithinRange(mapping, tp1(), "Z", swimlanesPerTopicPartition, 2 * swimlanesPerTopicPartition);

    }

    private void assertSwimlaneWithinRange(MultipleSwimlanesPerTopicPartitionMapping mapping, TopicPartition topicPartition, String key, int minInclusive, int maxExclusive) {
        assertThat(mapping.toSwimlane(topicPartition, key)).isGreaterThanOrEqualTo(minInclusive).isLessThan(maxExclusive);
    }


    private TopicPartition tp1() {
        return new TopicPartition("x", 1);
    }

    private TopicPartition tp0() {
        return new TopicPartition("x", 0);
    }

}