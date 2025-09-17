package io.eventuate.messaging.kafka.consumer;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SwimlanePerTopicPartitionTest {

    @Test
    public void shouldComputeSwimlane() {
        SwimlanePerTopicPartition mapping =  new SwimlanePerTopicPartition();
        assertEquals(Integer.valueOf(0), mapping.toSwimlane(tp0(), "X"));
        assertEquals(Integer.valueOf(0), mapping.toSwimlane(tp0(), "Y"));
        assertEquals(Integer.valueOf(1), mapping.toSwimlane(tp1(), "Z"));
    }

    private TopicPartition tp1() {
        return new TopicPartition("x", 1);
    }

    private TopicPartition tp0() {
        return new TopicPartition("x", 0);
    }

}