package io.eventuate.messaging.kafka.basic.consumer;

import org.apache.kafka.clients.consumer.*;
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
                                EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties) {
    this.subscriberId = subscriberId;
    this.handler = handler;
    this.topics = topics;

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

  public static List<PartitionInfo> verifyTopicExistsBeforeSubscribing(KafkaConsumer<String, byte[]> consumer, String topic) {
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

  private void maybeCommitOffsets(KafkaConsumer<String, byte[]> consumer, KafkaMessageProcessor processor) {
    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = processor.offsetsToCommit();
    if (!offsetsToCommit.isEmpty()) {
      consumerCallbacks.ifPresent(ConsumerCallbacks::onTryCommitCallback);
      logger.debug("Committing offsets {} {}", subscriberId, offsetsToCommit);
      consumer.commitSync(offsetsToCommit);
      logger.debug("Committed offsets {}", subscriberId);
      processor.noteOffsetsCommitted(offsetsToCommit);
      consumerCallbacks.ifPresent(ConsumerCallbacks::onCommitedCallback);
    }
  }

  public void start() {
    try {
      KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProperties);

      KafkaMessageProcessor processor = new KafkaMessageProcessor(subscriberId, handler);

      BackPressureManager backpressureManager = new BackPressureManager(backPressureConfig);

      for (String topic : topics) {
        verifyTopicExistsBeforeSubscribing(consumer, topic);
      }

      logger.debug("Subscribing to {} {}", subscriberId, topics);

      consumer.subscribe(new ArrayList<>(topics));

      logger.debug("Subscribed to {} {}", subscriberId, topics);

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


  private void runPollingLoop(KafkaConsumer<String, String> consumer, KafkaMessageProcessor processor, BackPressureManager backPressureManager) {
       int i=0;
      while (!stopFlag.get()) {
        try{
        ConsumerRecords<String, String> records = consumer.poll(100);
        if (!records.isEmpty())
          logger.debug("Got {} {} records", subscriberId, records.count());

        if (records.isEmpty())
          processor.throwFailureException();
        else
          for (ConsumerRecord<String, String> record : records) {
            logger.debug("processing record {} {} {}", subscriberId, record.offset(), record.value());
            if (logger.isDebugEnabled())
              logger.debug(String.format("EventuateKafkaAggregateSubscriptions subscriber = %s, offset = %d, key = %s, value = %s", subscriberId, record.offset(), record.key(), record.value()));
            processor.process(record);
          }
        if (!records.isEmpty())
          logger.debug("Processed {} {} records", subscriberId, records.count());

        maybeCommitOffsets(consumer, processor);

        if (!records.isEmpty())
          logger.debug("To commit {} {}", subscriberId, processor.getPending());

        int backlog = processor.backlog();

        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (ConsumerRecord<String, String> record : records) {
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
        i=0;
      } catch (KafkaMessageProcessorFailedException e) {
      // We are done
          i++;
         logger.debug("发生可重复异常 KafkaMessageProcessorFailedException {} 次 {}",i,e);
         if (i>10){
           logger.error("发生不可恢复错误，系统无法正常工作，需要人工停机检查！！ {}",e);
           throw e;
         }else {
           try{Thread.sleep(100*i);}catch (Exception e1){};
         }
        }catch (CommitFailedException e){
          i++;
          logger.debug("发生可重复异常 CommitFailedException {}次，{}",i,e);
          if (i>10){
            logger.error("发生不可恢复错误，系统无法正常工作，需要人工停机检查！！ {}",e);
            throw e;
          }else {
            try{Thread.sleep(100*i);}catch (Exception e1){};
          }
        } catch (Throwable e) {
          logger.error("发生不可恢复错误，系统无法正常工作，需要人工停机检查！！ {}",e);
      throw new RuntimeException(e);
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
