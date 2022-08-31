package io.eventuate.messaging.kafka.basic.consumer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class OffsetTrackerTest {

    @Test
    public void shouldTrackOffsets() {

        TopicPartition tp = new TopicPartition("x", 0);
        
        OffsetTracker offsetTracker = new OffsetTracker();

        offsetTracker.noteUnprocessed(tp, 1);
        offsetTracker.noteUnprocessed(tp, 2);
        offsetTracker.noteUnprocessed(tp, 3);

        offsetTracker.noteProcessed(tp, 2);

        assertThat(offsetTracker.offsetsToCommit()).isEmpty();

        offsetTracker.noteProcessed(tp, 1);

        Map<TopicPartition, OffsetAndMetadata> otc2 = offsetTracker.offsetsToCommit();

        assertEquals(makeExpectedOffsets(tp, 2), otc2);

        offsetTracker.noteOffsetsCommitted(otc2);

        assertThat(offsetTracker.offsetsToCommit()).isEmpty();

        offsetTracker.noteProcessed(tp, 3);

        Map<TopicPartition, OffsetAndMetadata> otc3 = offsetTracker.offsetsToCommit();
        assertEquals(makeExpectedOffsets(tp, 3), otc3);

        offsetTracker.noteOffsetsCommitted(otc3);

        assertThat(offsetTracker.offsetsToCommit()).isEmpty();
    }

    private Map<TopicPartition, OffsetAndMetadata> makeExpectedOffsets(TopicPartition tp, int offset) {
        return singletonMap(tp, new OffsetAndMetadata(offset + 1, ""));
    }

}