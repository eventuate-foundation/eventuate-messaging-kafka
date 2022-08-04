package io.eventuate.messaging.kafka.micronaut.consumer;

import io.eventuate.messaging.kafka.basic.consumer.EventuateKafkaConsumerConfigurationProperties;
import io.eventuate.messaging.kafka.basic.consumer.KafkaConsumerFactory;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.consumer.MessageConsumerKafkaImpl;
import io.eventuate.messaging.kafka.consumer.OriginalTopicPartitionToSwimLaneMapping;
import io.eventuate.messaging.kafka.consumer.TopicPartitionToSwimLaneMapping;
import io.micronaut.context.annotation.Factory;

import javax.inject.Singleton;

@Factory
public class MessageConsumerKafkaFactory {
  private TopicPartitionToSwimLaneMapping partitionToSwimLaneMapping = new OriginalTopicPartitionToSwimLaneMapping();

  @Singleton
  public MessageConsumerKafkaImpl messageConsumerKafka(EventuateKafkaConfigurationProperties props,
                                                       EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties,
                                                       KafkaConsumerFactory kafkaConsumerFactory) {
    return new MessageConsumerKafkaImpl(props.getBootstrapServers(), eventuateKafkaConsumerConfigurationProperties, kafkaConsumerFactory, partitionToSwimLaneMapping);
  }
}
