package io.eventuate.messaging.kafka.consumer;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

public class SwimlaneBasedDispatcher {

  private static Logger logger = LoggerFactory.getLogger(SwimlaneBasedDispatcher.class);

  private final ConcurrentHashMap<Integer, SwimlaneDispatcher> map = new ConcurrentHashMap<>();
  private Executor executor;
  private String subscriberId;

  private TopicPartitionToSwimLaneMapping partitionToSwimLaneMapping;

  public SwimlaneBasedDispatcher(String subscriberId, Executor executor, TopicPartitionToSwimLaneMapping partitionToSwimLaneMapping) {
    this.subscriberId = subscriberId;
    this.executor = executor;
    this.partitionToSwimLaneMapping = partitionToSwimLaneMapping;
  }

  public SwimlaneDispatcherBacklog dispatch(RawKafkaMessage message, TopicPartition topicPartition, Consumer<RawKafkaMessage> target) {
    SwimlaneDispatcher swimlaneDispatcher = getOrCreate(partitionToSwimLaneMapping.toSwimLane(topicPartition));
    return swimlaneDispatcher.dispatch(message, target);
  }

  private SwimlaneDispatcher getOrCreate(Integer swimlane) {
    SwimlaneDispatcher swimlaneDispatcher = map.get(swimlane);
    if (swimlaneDispatcher == null) {
      logger.trace("No dispatcher for {} {}. Attempting to create", subscriberId, swimlane);
      swimlaneDispatcher = new SwimlaneDispatcher(subscriberId, swimlane, executor);
      SwimlaneDispatcher r = map.putIfAbsent(swimlane, swimlaneDispatcher);
      if (r != null) {
        logger.trace("Using concurrently created SwimlaneDispatcher for {} {}", subscriberId, swimlane);
        swimlaneDispatcher = r;
      } else {
        logger.trace("Using newly created SwimlaneDispatcher for {} {}", subscriberId, swimlane);
      }
    }
    return swimlaneDispatcher;
  }
}

