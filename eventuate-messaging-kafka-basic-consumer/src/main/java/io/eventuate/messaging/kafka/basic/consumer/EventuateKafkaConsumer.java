package io.eventuate.messaging.kafka.basic.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Kafka consumer that manually commits offsets and supports asynchronous message processing
 */
public class EventuateKafkaConsumer {

  private static Logger logger = LoggerFactory.getLogger(EventuateKafkaConsumer.class);
  private final String subscriberId;
  private final EventuateKafkaConsumerMessageHandler handler;
  private final List<String> topics;
  private final KafkaConsumerFactory kafkaConsumerFactory;
  private final BackPressureConfig backPressureConfig;
  private final long pollTimeout;
  private AtomicBoolean stopFlag = new AtomicBoolean(false);
  private Properties consumerProperties;
  private volatile EventuateKafkaConsumerState state = EventuateKafkaConsumerState.CREATED;

  private volatile boolean closeConsumerOnStop = true;

  private Optional<ConsumerCallbacks> consumerCallbacks = Optional.empty();

  public EventuateKafkaConsumer(String subscriberId,
                                EventuateKafkaConsumerMessageHandler handler,
                                List<String> topics,
                                String bootstrapServers,
                                EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties,
                                KafkaConsumerFactory kafkaConsumerFactory) {
    this.subscriberId = subscriberId;
    this.handler = handler;
    this.topics = topics;
    this.kafkaConsumerFactory = kafkaConsumerFactory;

    this.consumerProperties = ConsumerPropertiesFactory.makeDefaultConsumerProperties(bootstrapServers, subscriberId);
    this.consumerProperties.putAll(eventuateKafkaConsumerConfigurationProperties.getProperties());
    this.backPressureConfig = eventuateKafkaConsumerConfigurationProperties.getBackPressure();
    this.pollTimeout = eventuateKafkaConsumerConfigurationProperties.getPollTimeout();
  }

  public void setConsumerCallbacks(Optional<ConsumerCallbacks> consumerCallbacks) {
    this.consumerCallbacks = consumerCallbacks;
  }

  public boolean isCloseConsumerOnStop() {
    return closeConsumerOnStop;
  }

  public void setCloseConsumerOnStop(boolean closeConsumerOnStop) {
    this.closeConsumerOnStop = closeConsumerOnStop;
  }

  public static List<PartitionInfo> verifyTopicExistsBeforeSubscribing(KafkaMessageConsumer consumer, String topic) {
    try {
      logger.debug("Verifying Topic {}", topic);
      List<PartitionInfo> partitions = consumer.partitionsFor(topic);
      logger.debug("Got these partitions {} for Topic {}", partitions, topic);
      return partitions;
    } catch (Throwable e) {
      logger.error("Got exception: ", e);
      throw new RuntimeException(e);
    }
  }

  private void maybeCommitOffsets(KafkaMessageConsumer consumer, KafkaMessageProcessor processor) {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = processor.offsetsToCommit();
    if (!offsetsToCommit.isEmpty()) {
      consumerCallbacks.ifPresent(ConsumerCallbacks::onTryCommitCallback);
      logger.debug("Committing offsets {} {}", subscriberId, offsetsToCommit);
      consumer.commitOffsets(offsetsToCommit);
      logger.debug("Committed offsets {}", subscriberId);
      processor.noteOffsetsCommitted(offsetsToCommit);
      consumerCallbacks.ifPresent(ConsumerCallbacks::onCommitedCallback);
    }
  }

  public void start() {
    try {
      KafkaMessageConsumer consumer = kafkaConsumerFactory.makeConsumer(subscriberId, consumerProperties);

      KafkaMessageProcessor processor = new KafkaMessageProcessor(subscriberId, handler);

      BackPressureManager backpressureManager = new BackPressureManager(backPressureConfig);

      for (String topic : topics) {
        verifyTopicExistsBeforeSubscribing(consumer, topic);
      }

      subscribe(consumer);

      new Thread(() -> {

        try {
          runPollingLoop(consumer, processor, backpressureManager);

          maybeCommitOffsets(consumer, processor);

          state = EventuateKafkaConsumerState.STOPPED;

          if (closeConsumerOnStop) {
            consumer.close();
          }

        } catch (KafkaMessageProcessorFailedException e) {
          // We are done
          logger.trace("Terminating since KafkaMessageProcessorFailedException");
          state = EventuateKafkaConsumerState.MESSAGE_HANDLING_FAILED;
          consumer.close(Duration.of(200, ChronoUnit.MILLIS));
        } catch (Throwable e) {
          logger.error("Got exception: ", e);
          state = EventuateKafkaConsumerState.FAILED;
          consumer.close(Duration.of(200, ChronoUnit.MILLIS));
          throw new RuntimeException(e);
        }
        logger.trace("Stopped in state {}", state);

      }, "Eventuate-subscriber-" + subscriberId).start();

      state = EventuateKafkaConsumerState.STARTED;

    } catch (Exception e) {
      logger.error("Error subscribing", e);
      state = EventuateKafkaConsumerState.FAILED_TO_START;
      throw new RuntimeException(e);
    }
  }

  private void subscribe(KafkaMessageConsumer consumer) {
    logger.debug("Subscribing to {} {}", subscriberId, topics);
    consumer.subscribe(topics);
    logger.debug("Subscribed to {} {}", subscriberId, topics);
  }

  private void runPollingLoop(KafkaMessageConsumer consumer, KafkaMessageProcessor processor, BackPressureManager backPressureManager) {
    while (!stopFlag.get()) {
      ConsumerRecords<String, byte[]> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
      if (!records.isEmpty())
        logger.debug("Got {} {} records", subscriberId, records.count());

      if (records.isEmpty())
        processor.throwFailureException();
      else
        for (ConsumerRecord<String, byte[]> record : records) {
          logger.debug("processing record subscriberId={} tpo=({} {} {}) body={}", subscriberId, record.topic(), record.partition(), record.offset(), record.value());
          if (logger.isDebugEnabled())
            logger.debug(String.format("EventuateKafkaAggregateSubscriptions subscriber = %s, offset = %d, key = %s, value = %s", subscriberId, record.offset(), record.key(), record.value()));
          processor.process(record);
        }
      if (!records.isEmpty())
        logger.debug("Processed {} {} records", subscriberId, records.count());

      try {
        maybeCommitOffsets(consumer, processor);
      } catch (Exception e) {
        logger.error("Cannot commit offsets", e);
        consumerCallbacks.ifPresent(ConsumerCallbacks::onCommitFailedCallback);
      }

      if (!records.isEmpty())
        logger.debug("To commit {} {}", subscriberId, processor.getPending());

      int backlog = processor.backlog();

      Set<TopicPartition> topicPartitions = new HashSet<>();
      for (ConsumerRecord<String, byte[]> record : records) {
        topicPartitions.add(new TopicPartition(record.topic(), record.partition()));
      }
      BackPressureActions actions = backPressureManager.update(topicPartitions, backlog);

      if (!actions.pause.isEmpty()) {
        logger.info("Subscriber {} pausing {} due to backlog {} > {}", subscriberId, actions.pause, backlog, backPressureConfig.getHigh());
        consumer.pause(actions.pause);
      }

      if (!actions.resume.isEmpty()) {
        logger.info("Subscriber {} resuming {} due to backlog {} <= {}", subscriberId, actions.resume, backlog, backPressureConfig.getLow());
        consumer.resume(actions.resume);
      }


    }
  }

  public void stop() {
    stopFlag.set(true);
//    can't call consumer.close(), it is not thread safe,
//    it can produce java.util.ConcurrentModificationException: KafkaConsumer is not safe for multi-threaded access
  }

  public EventuateKafkaConsumerState getState() {
    return state;
  }
}
