package io.eventuate.messaging.kafka.basic.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.HashSet;
import java.util.Set;

public class BackPressureManager {

  private final BackPressureConfig backPressureConfig;
  private Set<TopicPartition> allTopicPartitions = new HashSet<>();

  private BackPressureManagerState state = new BackPressureManagerNormalState();

  public BackPressureManager(BackPressureConfig backPressureConfig) {
    this.backPressureConfig = backPressureConfig;
  }

  public BackPressureActions update(ConsumerRecords<String, String> records, int backlog) {

    for (ConsumerRecord<String, String> record : records) {
      allTopicPartitions.add(new TopicPartition(record.topic(), record.partition()));
    }
    BackPressureManagerStateAndActions stateAndActions = state.update(allTopicPartitions, backlog, backPressureConfig);
    this.state = stateAndActions.state;
    return stateAndActions.actions;
  }


}
