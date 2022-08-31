package io.eventuate.messaging.kafka.basic.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Processes a Kafka message and tracks the message offsets that have been successfully processed and can be committed
 */
public class KafkaMessageProcessor {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final String subscriberId;
  private final EventuateKafkaConsumerMessageHandler handler;
  private final OffsetTracker offsetTracker = new OffsetTracker();

  private final BlockingQueue<ConsumerRecord<String, byte[]>> processedRecords = new LinkedBlockingQueue<>();
  private final AtomicReference<KafkaMessageProcessorFailedException> failed = new AtomicReference<>();

  private final Set<MessageConsumerBacklog> consumerBacklogs = new HashSet<>();

  public KafkaMessageProcessor(String subscriberId, EventuateKafkaConsumerMessageHandler handler) {
    this.subscriberId = subscriberId;
    this.handler = handler;
  }


  public void process(ConsumerRecord<String, byte[]> record) {
    throwFailureException();
    offsetTracker.noteUnprocessed(new TopicPartition(record.topic(), record.partition()), record.offset());
    MessageConsumerBacklog consumerBacklog = handler.apply(record, (result, t) -> {
      handleMessagingHandlingOutcome(record, t);
    });
    if (consumerBacklog != null)
      consumerBacklogs.add(consumerBacklog);
  }

  private void handleMessagingHandlingOutcome(ConsumerRecord<String, byte[]> record, Throwable t) {
    if (t != null) {
      logger.error("Got exception: ", t);
      failed.set(new KafkaMessageProcessorFailedException(t));
    } else {
      logger.debug("Adding processed record to queue {} {}", subscriberId, record.offset());
      processedRecords.add(record);
    }
  }

  void throwFailureException() {
    if (failed.get() != null)
      throw failed.get();
  }

  public Map<TopicPartition, OffsetAndMetadata> offsetsToCommit() {
    int count = 0;
    while (true) {
      ConsumerRecord<String, byte[]> record = processedRecords.poll();
      if (record == null)
        break;
      count++;
      offsetTracker.noteProcessed(new TopicPartition(record.topic(), record.partition()), record.offset());
    }
    logger.trace("removed {} {} processed records from queue", subscriberId, count);
    return offsetTracker.offsetsToCommit();
  }

  public void noteOffsetsCommitted(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
    offsetTracker.noteOffsetsCommitted(offsetsToCommit);
  }

  public OffsetTracker getPending() {
    return offsetTracker;
  }

  public int backlog() {
    return consumerBacklogs.stream().mapToInt(MessageConsumerBacklog::size).sum();
  }

}
