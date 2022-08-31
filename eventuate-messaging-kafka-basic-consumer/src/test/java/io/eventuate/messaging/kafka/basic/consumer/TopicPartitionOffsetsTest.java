package io.eventuate.messaging.kafka.basic.consumer;

import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TopicPartitionOffsetsTest  {

  @Test
  public void shouldTrackOffsets() {

    TopicPartitionOffsets tpo = new TopicPartitionOffsets();

    tpo.noteUnprocessed(1);
    tpo.noteUnprocessed(2);
    tpo.noteUnprocessed(3);

    tpo.noteProcessed(2);

    assertFalse(tpo.offsetToCommit().isPresent());

    tpo.noteProcessed(1);

    assertEquals(Long.valueOf(2L), tpo.offsetToCommit().get());

    tpo.noteOffsetCommitted(2);

    assertEquals(Optional.empty(), tpo.offsetToCommit());

    tpo.noteProcessed(3);

    assertEquals(Long.valueOf(3), tpo.offsetToCommit().get());

    tpo.noteOffsetCommitted(3);

    assertEquals(Optional.empty(), tpo.offsetToCommit());
  }


}
