package io.eventuate.messaging.kafka.basic.consumer;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Keeps track of message offsets that are (a) being processed and (b) have been processed and can be committed
 */
public class OffsetTracker {

  private Map<TopicPartition, TopicPartitionOffsets> state = new HashMap<>();

  @Override
  public String toString() {
    return new ToStringBuilder(this)
            .append("state", state)
            .toString();
  }

   private TopicPartitionOffsets fetch(TopicPartition topicPartition) {
    TopicPartitionOffsets tpo = state.get(topicPartition);
    if (tpo == null) {
      tpo = new TopicPartitionOffsets();
      state.put(topicPartition, tpo);
    }
    return tpo;
  }
  void noteUnprocessed(TopicPartition topicPartition, long offset) {
    fetch(topicPartition).noteUnprocessed(offset);
  }

  void noteProcessed(TopicPartition topicPartition, long offset) {
    fetch(topicPartition).noteProcessed(offset);
  }

  private final int OFFSET_ADJUSTMENT = 1;
  public Map<TopicPartition, OffsetAndMetadata> offsetsToCommit() {
    Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
    state.forEach((tp, tpo) -> tpo.offsetToCommit().ifPresent(offset -> result.put(tp, new OffsetAndMetadata(offset + OFFSET_ADJUSTMENT, ""))));
    return result;
  }

  public void noteOffsetsCommitted(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
    offsetsToCommit.forEach((tp, om) -> {
      fetch(tp).noteOffsetCommitted(om.offset() - OFFSET_ADJUSTMENT);
    });
  }

  public Map<TopicPartition, Set<Long>> getPending() {
    Map<TopicPartition, Set<Long>> result = new HashMap<>();
    state.forEach((tp, tpo) -> {
      Set<Long> pending = tpo.getPending();
      if (!pending.isEmpty())
        result.put(tp, pending);
    });
    return result;
  }
}
